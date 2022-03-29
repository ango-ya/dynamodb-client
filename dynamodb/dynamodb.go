package dynamodb

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/ango-ya/aws-s3-client/s3"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	// "github.com/davecgh/go-spew/spew"
)

var (
	ErrUnsupported     = errors.New("unsupported")
	ErrUnexpectedInput = errors.New("unexpected input")
	ErrNothingToCommit = errors.New("nothing to commit")
	ErrKeyTagNotFound  = errors.New("key tag not found")
)

type DynamoDB struct {
	cfg    aws.Config
	client *dynamodb.Client

	s3client *s3.S3Client

	logger  zerolog.Logger
	middles []Middleware

	// configs
	timeout time.Duration
}

func NewDynamoDB(ctx context.Context, confOpts []func(*config.LoadOptions) error, opts ...Option) (d DynamoDB, err error) {
	if d.cfg, err = config.LoadDefaultConfig(ctx, confOpts...); err != nil {
		return
	}

	d.client = dynamodb.NewFromConfig(d.cfg)

	d.logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
	d.timeout = DefaultTimeout

	for i := range opts {
		opts[i].Apply(&d)
	}

	d.middles = []Middleware{
		d.printLog,
		d.setTimeout,
	}

	return
}

func (d *DynamoDB) SetS3(c *s3.S3Client) {
	d.s3client = c
}

func (d *DynamoDB) CreateTable(ctx context.Context, arg *dynamodb.CreateTableInput) (out interface{}, err error) {
	handler := func(ctx context.Context, in interface{}) (out interface{}, err error) {
		return d.client.CreateTable(ctx, arg)
	}

	return d.invoke(ctx, arg, handler)
}

func (d *DynamoDB) DropTable(ctx context.Context, arg *dynamodb.DeleteTableInput) (out interface{}, err error) {
	handler := func(ctx context.Context, in interface{}) (out interface{}, err error) {
		return d.client.DeleteTable(ctx, arg)
	}

	return d.invoke(ctx, arg, handler)
}

func (d *DynamoDB) Get(ctx context.Context, arg *dynamodb.GetItemInput) (out interface{}, err error) {
	handler := func(ctx context.Context, in interface{}) (out interface{}, err error) {
		return d.client.GetItem(ctx, arg)
	}

	return d.invoke(ctx, arg, handler)
}

func (d *DynamoDB) Scan(ctx context.Context, arg *dynamodb.ScanInput) (out interface{}, err error) {
	handler := func(ctx context.Context, in interface{}) (out interface{}, err error) {
		return d.client.Scan(ctx, arg)
	}

	return d.invoke(ctx, arg, handler)
}

func (d *DynamoDB) DescribeTable(ctx context.Context, arg *dynamodb.DescribeTableInput) (out interface{}, err error) {
	handler := func(ctx context.Context, in interface{}) (out interface{}, err error) {
		return d.client.DescribeTable(ctx, arg)
	}

	return d.invoke(ctx, arg, handler)
}

func (d *DynamoDB) CreateTableFromStruct(ctx context.Context, in interface{}) (out interface{}, err error) {
	v, err := d.acquireReflectValue(in)
	if err != nil {
		return
	}

	defs, ks, err := attrDefsAndKyeSchemas(in)
	if err != nil {
		return
	}

	tableName := name(v.Type())
	arg := &dynamodb.CreateTableInput{
		AttributeDefinitions: defs,
		KeySchema:            ks,
		TableName:            aws.String(tableName),
		BillingMode:          types.BillingModeProvisioned,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	}

	d.logger.Info().Msgf("creating table: %s", tableName)
	return d.CreateTable(ctx, arg)
}

func (d *DynamoDB) DropTableFromStruct(ctx context.Context, in interface{}) (out interface{}, err error) {
	v, err := d.acquireReflectValue(in)
	if err != nil {
		return
	}

	tableName := name(v.Type())
	arg := &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	}

	d.logger.Info().Msgf("droping table: %s", tableName)
	return d.DropTable(ctx, arg)
}

func (d *DynamoDB) GetStruct(ctx context.Context, in interface{}, result interface{}) (err error) {
	v, err := d.acquireReflectValue(in)
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

	arg := &dynamodb.GetItemInput{
		TableName: aws.String(name(v.Type())),
		Key:       key,
	}

	out, err := d.Get(ctx, arg)
	if err != nil {
		return
	}

	res := out.(*dynamodb.GetItemOutput)

	return attributevalue.UnmarshalMap(res.Item, result)
}

func (d *DynamoDB) ScanStruct(ctx context.Context, in interface{}, result interface{}) (err error) {
	v, err := d.acquireReflectValue(in)
	if err != nil {
		return
	}

	arg := &dynamodb.ScanInput{
		TableName: aws.String(name(v.Type())),
	}

	out, err := d.Scan(ctx, arg)
	if err != nil {
		return
	}

	res := out.(*dynamodb.ScanOutput)

	return attributevalue.UnmarshalListOfMaps(res.Items, result)
}

func (d *DynamoDB) DescribeTableFromStruct(ctx context.Context, in interface{}) (out interface{}, err error) {
	v, err := d.acquireReflectValue(in)
	if err != nil {
		return
	}

	arg := &dynamodb.DescribeTableInput{
		TableName: aws.String(name(v.Type())),
	}

	return d.DescribeTable(ctx, arg)
}

func (d *DynamoDB) WaitForTableCreation(ctx context.Context, in interface{}) error {
	v, err := d.acquireReflectValue(in)
	if err != nil {
		return err
	}

	w := dynamodb.NewTableExistsWaiter(d.client)
	return w.Wait(ctx,
		&dynamodb.DescribeTableInput{
			TableName: aws.String(name(v.Type())),
		},
		1*time.Minute,
		func(o *dynamodb.TableExistsWaiterOptions) {
			o.MaxDelay = 10 * time.Second
			o.MinDelay = 1 * time.Second
		},
	)
}

func (d *DynamoDB) WaitForTableDeletion(ctx context.Context, in interface{}) error {
	v, err := d.acquireReflectValue(in)
	if err != nil {
		return err
	}

	w := dynamodb.NewTableNotExistsWaiter(d.client)
	return w.Wait(ctx,
		&dynamodb.DescribeTableInput{
			TableName: aws.String(name(v.Type())),
		},
		1*time.Minute,
		func(o *dynamodb.TableNotExistsWaiterOptions) {
			o.MaxDelay = 10 * time.Second
			o.MinDelay = 1 * time.Second
		},
	)
}

func (d *DynamoDB) Writer(ctx context.Context) BatchWriter {
	return BatchWriter{}
}

func (d *DynamoDB) Commit(ctx context.Context, writer *BatchWriter) (out interface{}, err error) {
	out, err = d.commit(ctx, writer.Contents())
	return
}

func (d *DynamoDB) WriterWithS3(ctx context.Context) (b BatchWriterWithS3) {
	b.s3client = d.s3client
	b.uploadeds = make(map[PairOfBucketNameAndKey]string)
	return
}

func (d *DynamoDB) CommitWithS3(ctx context.Context, writer *BatchWriterWithS3) (out interface{}, err error) {
	if out, err = d.commit(ctx, writer.Contents()); err != nil {
		// delete uploaded files from s3
		for pair, url := range writer.uploadeds {
			if delErr := d.s3client.Delete(ctx, pair.Name, pair.Key); delErr != nil {
				err = errors.Wrap(err, fmt.Sprintf("failed delete file(%s)", url))
			}
		}
		return
	}

	return
}

func (b *DynamoDB) acquireReflectValue(in interface{}) (v reflect.Value, err error) {
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

func (d *DynamoDB) invoke(ctx context.Context, in interface{}, h Handler) (out interface{}, err error) {
	handler := applyMiddlewares(h, d.middles)
	return handler(ctx, in)
}

func (d *DynamoDB) commit(ctx context.Context, items []types.TransactWriteItem) (out interface{}, err error) {
	if len(items) == 0 {
		err = ErrNothingToCommit
		return
	}

	tx := &dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	}

	handler := func(ctx context.Context, in interface{}) (out interface{}, err error) {
		return d.client.TransactWriteItems(ctx, tx)
	}

	d.logger.Info().Msgf("commiting %d numbers of data", len(items))

	// retry 3 times
	for i := 0; i < 3; i++ {
		out, err = d.invoke(ctx, tx, handler)
		if err == nil {
			break
		}
	}

	return
}

func IsExistingTable(err error) (bool, error) {
	if err != nil {
		var target *types.ResourceNotFoundException
		if errors.As(err, &target) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func attrDefsAndKyeSchemas(s interface{}) (defs []types.AttributeDefinition, ks []types.KeySchemaElement, err error) {
	t := reflect.TypeOf(s)
	v := reflect.ValueOf(s)
	for i := 0; i < t.NumField(); i++ {
		var (
			fieldType  = t.Field(i)
			fieldValue = v.Field(i)
			attrName   = name(fieldType)
			attrType   types.ScalarAttributeType
		)
		if _, exist := fieldType.Tag.Lookup(KEY_TAG); !exist {
			continue
		}

		switch fieldValue.Kind() {
		case reflect.Bool:
			attrType = types.ScalarAttributeTypeB
		case reflect.String:
			attrType = types.ScalarAttributeTypeS
		case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			attrType = types.ScalarAttributeTypeN
		default:
			err = ErrUnsupported
			return
		}

		defs = append(defs, types.AttributeDefinition{
			AttributeName: &attrName,
			AttributeType: attrType,
		})

		ks = append(ks, types.KeySchemaElement{
			AttributeName: &attrName,
			KeyType:       types.KeyTypeHash,
		})
	}

	if len(defs) == 0 {
		err = ErrKeyTagNotFound
	}

	return
}
