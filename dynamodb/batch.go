package dynamodb

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/iancoleman/strcase"
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

func (b *BatchWriter) Contents() interface{} {
	b.Lock()
	defer b.Unlock()

	return b.inputs
}

func (b *BatchWriter) Len() int {
	b.Lock()
	defer b.Unlock()

	return len(b.inputs)
}

func (b *BatchWriter) InsertStruct(in interface{}) (err error) {
	v := reflect.ValueOf(in)

	// only struct supported for now
	if v.Kind() != reflect.Struct {
		return ErrUnsupported
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

	return b.Put(context.Background(), tx)
}

func (b *BatchWriter) DeleteStruct(in interface{}) (err error) {
	v := reflect.ValueOf(in)

	// only struct supported for now
	if v.Kind() != reflect.Struct {
		return ErrUnsupported
	}

	key, exist, err := lookupKey(in)
	if err != nil && !exist {
		err = ErrKeyNotFound
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

	return b.Put(context.Background(), tx)
}

func (b *BatchWriter) UpdateStruct(in interface{}) (err error) {
	v := reflect.ValueOf(in)

	// only struct supported for now
	if v.Kind() != reflect.Struct {
		return ErrUnsupported
	}

	key, exist, err := lookupKey(in)
	if err != nil && !exist {
		err = ErrKeyNotFound
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

	return b.Put(context.Background(), tx)
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

func lookupKey(s interface{}) (attrMap map[string]types.AttributeValue, exist bool, err error) {
	attrMap = make(map[string]types.AttributeValue)
	t := reflect.TypeOf(s)
	v := reflect.ValueOf(s)
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
		err = ErrKeyNotFound
	}

	return
}
