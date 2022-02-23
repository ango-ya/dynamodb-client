package dynamodb

import (
	"context"
	"os"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

var (
	ErrUnsupported     = errors.New("unsupported")
	ErrUnexpectedInput = errors.New("unexpected input")
	ErrNothingToCommit = errors.New("nothing to commit")
	ErrKeyNotFound     = errors.New("key not found")
)

type DynamoDB struct {
	cfg    aws.Config
	client *dynamodb.Client

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

func (d *DynamoDB) List(ctx context.Context, arg *dynamodb.ScanInput) (out interface{}, err error) {
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

func (d *DynamoDB) Writer(ctx context.Context) (out BatchWriter, err error) {
	return BatchWriter{}, nil
}

func (d *DynamoDB) Commit(ctx context.Context, writer *BatchWriter) (out interface{}, err error) {
	if writer.Len() == 0 {
		err = ErrNothingToCommit
		return
	}

	items, ok := writer.Contents().([]types.TransactWriteItem)
	if !ok {
		return nil, ErrUnexpectedInput
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

func (d *DynamoDB) invoke(ctx context.Context, in interface{}, h Handler) (out interface{}, err error) {
	handler := applyMiddlewares(h, d.middles)
	return handler(ctx, in)
}

func (d *DynamoDB) CreateTableFromStruct(ctx context.Context, in interface{}) (out interface{}, err error) {
	v := reflect.ValueOf(in)

	// only struct supported for now
	if v.Kind() != reflect.Struct {
		err = ErrUnsupported
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
	v := reflect.ValueOf(in)

	// only struct supported for now
	if v.Kind() != reflect.Struct {
		err = ErrUnsupported
		return
	}

	tableName := name(v.Type())
	arg := &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	}

	d.logger.Info().Msgf("droping table: %s", tableName)
	return d.DropTable(ctx, arg)
}

func (d *DynamoDB) DescribeTableFromStruct(ctx context.Context, in interface{}) (out interface{}, err error) {
	v := reflect.ValueOf(in)

	// only struct supported for now
	if v.Kind() != reflect.Struct {
		err = ErrUnsupported
		return
	}

	arg := &dynamodb.DescribeTableInput{
		TableName: aws.String(name(v.Type())),
	}

	return d.DescribeTable(ctx, arg)
}

func (d *DynamoDB) WaitForTableCreation(ctx context.Context, in interface{}) error {
	v := reflect.ValueOf(in)

	// only struct supported for now
	if v.Kind() != reflect.Struct {
		return ErrUnsupported
	}

	w := dynamodb.NewTableExistsWaiter(d.client)
	err := w.Wait(ctx,
		&dynamodb.DescribeTableInput{
			TableName: aws.String(name(v.Type())),
		},
		1*time.Minute,
		func(o *dynamodb.TableExistsWaiterOptions) {
			o.MaxDelay = 10 * time.Second
			o.MinDelay = 1 * time.Second
		},
	)

	return err
}

func (d *DynamoDB) WaitForTableDeletion(ctx context.Context, in interface{}) error {
	v := reflect.ValueOf(in)

	// only struct supported for now
	if v.Kind() != reflect.Struct {
		return ErrUnsupported
	}

	w := dynamodb.NewTableNotExistsWaiter(d.client)
	err := w.Wait(ctx,
		&dynamodb.DescribeTableInput{
			TableName: aws.String(name(v.Type())),
		},
		1*time.Minute,
		func(o *dynamodb.TableNotExistsWaiterOptions) {
			o.MaxDelay = 10 * time.Second
			o.MinDelay = 1 * time.Second
		},
	)

	return err
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
		err = ErrKeyNotFound
	}

	return
}
