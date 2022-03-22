package dynamodb

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/ango-ya/aws-s3-client/s3"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/iancoleman/strcase"
	"github.com/pkg/errors"
	// "github.com/davecgh/go-spew/spew"
)

const (
	KEY_TAG = "key"
)

type BatchWriter struct {
	sync.Mutex
	inputs []types.TransactWriteItem
}

func (b *BatchWriter) Put(ctx context.Context, in interface{}) (err error) {
	b.Lock()
	defer b.Unlock()

	item, ok := in.(types.TransactWriteItem)
	if !ok {
		return ErrUnexpectedInput
	}

	b.inputs = append(b.inputs, item)
	return
}

func (b *BatchWriter) Contents() []types.TransactWriteItem {
	b.Lock()
	defer b.Unlock()

	return b.inputs
}

func (b *BatchWriter) Len() int {
	b.Lock()
	defer b.Unlock()

	return len(b.inputs)
}

func (b *BatchWriter) InsertStruct(ctx context.Context, in interface{}) (err error) {
	v, err := b.acquireValue(in)
	if err != nil {
		return
	}

	item, err := attributevalue.MarshalMap(in)
	if err != nil {
		return
	}

	tx := types.TransactWriteItem{
		Put: &types.Put{
			TableName: aws.String(name(v.Type())),
			Item:      item,
		},
	}

	return b.Put(ctx, tx)
}

func (b *BatchWriter) DeleteStruct(ctx context.Context, in interface{}) (err error) {
	v, err := b.acquireValue(in)
	if err != nil {
		return
	}

	key, exist, err := lookupKey(v)
	if err != nil && !exist {
		err = ErrKeyTagNotFound
	}
	if err != nil {
		return
	}

	tx := types.TransactWriteItem{
		Delete: &types.Delete{
			TableName: aws.String(name(v.Type())),
			Key:       key,
		},
	}

	return b.Put(ctx, tx)
}

func (b *BatchWriter) UpdateStruct(ctx context.Context, in interface{}) (err error) {
	v, err := b.acquireValue(in)
	if err != nil {
		return
	}

	key, exist, err := lookupKey(v)
	if err != nil && !exist {
		err = ErrKeyTagNotFound
	}
	if err != nil {
		return
	}

	attrMap, err := attributevalue.MarshalMap(in)
	if err != nil {
		return
	}

	expression := "SET "
	expressionAttr := make(map[string]types.AttributeValue, len(attrMap))
	for k := range attrMap {
		if _, exist := key[k]; exist {
			continue
		}
		expression += fmt.Sprintf("%s = :%s, ", k, k)
		expressionAttr[":"+k] = attrMap[k]
	}

	expression = expression[:len(expression)-2]

	tx := types.TransactWriteItem{
		Update: &types.Update{
			TableName:                 aws.String(name(v.Type())),
			Key:                       key,
			UpdateExpression:          aws.String(expression),
			ExpressionAttributeValues: expressionAttr,
		},
	}

	return b.Put(ctx, tx)
}

func (b *BatchWriter) acquireValue(in interface{}) (v reflect.Value, err error) {
	v = reflect.ValueOf(in)

	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	// only struct supported for now
	if v.Kind() != reflect.Struct {
		err = ErrUnsupported
	}

	return
}

type PairOfBucketNameAndKey struct {
	Name string
	Key  string
}

type BatchWriterWithS3 struct {
	BatchWriter

	s3client  *s3.S3Client
	uploadeds map[PairOfBucketNameAndKey]string
}

func (b *BatchWriterWithS3) InsertStructWithS3(ctx context.Context, in interface{}, fieldName, bucket, key string, body io.Reader) error {
	if err := b.upload(ctx, in, fieldName, bucket, key, body); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to inserting %v to s3 batch", in))
	}
	return b.InsertStruct(ctx, in)
}

func (b *BatchWriterWithS3) UpdateStructWithS3(ctx context.Context, in interface{}, fieldName, bucket, key string, body io.Reader) error {
	if err := b.upload(ctx, in, fieldName, bucket, key, body); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to updating %v on s3 batch", in))
	}
	return b.UpdateStruct(ctx, in)
}

func (b *BatchWriterWithS3) upload(ctx context.Context, in interface{}, fieldName, bucket, key string, body io.Reader) (err error) {
	v, err := b.acquireValue(in)
	if err != nil {
		return err
	}

	url, err := b.s3client.Put(ctx, bucket, key, body)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to upload, bucket(%s), key(%s)", bucket, key))
	}

	if err = setS3URL(v, fieldName, url); err != nil {
		err = errors.Wrap(err, "failed to set")
		if delErr := b.s3client.Delete(ctx, bucket, key); delErr != nil {
			err = errors.Wrap(err, fmt.Sprintf("failed delete file(%s)", url))
		}
		return err
	}

	b.Lock()
	b.uploadeds[PairOfBucketNameAndKey{Name: bucket, Key: key}] = url
	b.Unlock()

	return nil
}

func (b *BatchWriterWithS3) acquireValue(in interface{}) (v reflect.Value, err error) {
	v = reflect.ValueOf(in)
	if v.Kind() != reflect.Ptr {
		err = errors.Wrap(ErrUnsupported, "only pointer is supported")
		return
	}

	if v.Elem().Kind() != reflect.Struct {
		err = errors.Wrap(ErrUnsupported, "only struct is supporting")
	}

	return
}

func name(in interface{}) string {
	var n string
	switch v := in.(type) {
	case reflect.Type:
		n = v.Name()
	case reflect.StructField:
		n = v.Name
	default:
		n = ""
	}
	return strcase.ToKebab(n)
}

func lookupKey(v reflect.Value) (attrMap map[string]types.AttributeValue, exist bool, err error) {
	attrMap = make(map[string]types.AttributeValue)
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		var (
			fieldType  = t.Field(i)
			fieldValue = v.Field(i)
		)
		if _, exist = fieldType.Tag.Lookup(KEY_TAG); !exist {
			continue
		}

		if attrMap[name(fieldType)], err = attributevalue.Marshal(fieldValue.Interface()); err != nil {
			return
		}
	}

	if len(attrMap) == 0 {
		err = ErrKeyTagNotFound
	}

	return
}

func setS3URL(value reflect.Value, fieldName, s3URL string) error {
	v := value.Elem()
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		var (
			fieldValue = v.Field(i)
			fieldType  = t.Field(i)
		)

		if !isSameName(fieldType.Name, fieldName) {
			continue
		}
		if fieldValue.Kind() != reflect.String {
			return errors.Wrap(ErrUnsupported, fmt.Sprintf("the target filed(%s) should be string", fieldName))
		}

		if !fieldValue.CanSet() {
			panic(fmt.Sprintf("unexpected: fieldValue(%v), value(%v), fieldName(%v)", fieldValue, value, fieldName))
		}
		fieldValue.SetString(s3URL)
	}

	return nil
}

func isSameName(a, b string) bool {
	return strcase.ToCamel(a) == strcase.ToCamel(b)
}
