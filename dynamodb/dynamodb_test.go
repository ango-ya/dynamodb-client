package dynamodb

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/ango-ya/aws-s3-client/s3"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

const (
	TestRegion  = "us-east-1"
	TestProfile = "sto-dev"
)

type Persons struct {
	Id   string `key:"-" dynamodbav:"id"`
	Name string `dynamodbav:"name"`
	Age  int    `dynamodbav:"age"`
}

func TestAll(t *testing.T) {
	var (
		ctx       = context.Background()
		tableName = "persons"
		confOpts  = []func(*config.LoadOptions) error{
			config.WithDefaultRegion(TestRegion),
			config.WithSharedConfigProfile(TestProfile),
		}
		persons = []Persons{
			Persons{"1", "tom", 30},
			Persons{"2", "tim", 40},
			Persons{"3", "tik", 50},
		}
		err error
	)

	d, err := NewDynamoDB(ctx, confOpts, WithTimeout(5*time.Second))
	require.NoError(t, err)

	// create table
	createArg := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       types.KeyTypeHash,
			},
		},
		TableName:   aws.String(tableName),
		BillingMode: types.BillingModeProvisioned,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	}
	_, err = d.CreateTable(ctx, createArg)
	require.NoError(t, err)

	// sleep until table created
	time.Sleep(10 * time.Second)

	// put
	batch := d.Writer(ctx)
	for _, person := range persons {
		item := types.TransactWriteItem{
			Put: &types.Put{
				TableName: aws.String(tableName),
				Item: map[string]types.AttributeValue{
					"id":   &types.AttributeValueMemberS{Value: person.Id},
					"name": &types.AttributeValueMemberS{Value: person.Name},
					"age":  &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", person.Age)},
				},
			},
		}
		batch.Put(ctx, item)
	}
	_, err = d.Commit(ctx, &batch)
	require.NoError(t, err)

	// list
	listArg := &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	}
	listOut, err := d.Scan(ctx, listArg)
	require.NoError(t, err)

	resp := listOut.(*dynamodb.ScanOutput)
	require.Equal(t, len(persons), int(resp.ScannedCount))

	var result []Persons
	err = attributevalue.UnmarshalListOfMaps(resp.Items, &result)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal Dynamodb Scan Items, %v", err))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Id < result[j].Id
	})
	require.EqualValues(t, persons, result)

	err = d.ScanStruct(ctx, persons[0], &result)
	require.NoError(t, err)
	sort.Slice(result, func(i, j int) bool {
		return result[i].Id < result[j].Id
	})
	require.EqualValues(t, persons, result)

	// update & delete
	batch = d.Writer(ctx)
	item := types.TransactWriteItem{
		Update: &types.Update{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: persons[0].Id},
			},
			UpdateExpression: aws.String("SET age = :age"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":age": &types.AttributeValueMemberN{Value: "100"},
			},
		},
	}
	batch.Put(ctx, item)
	item = types.TransactWriteItem{
		Delete: &types.Delete{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: persons[1].Id},
			},
		},
	}
	batch.Put(ctx, item)
	_, err = d.Commit(ctx, &batch)
	require.NoError(t, err)

	// get
	getArg := &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: persons[0].Id},
		},
	}
	getOut, err := d.Get(ctx, getArg)
	require.NoError(t, err)

	res := getOut.(*dynamodb.GetItemOutput)

	var p Persons
	err = attributevalue.UnmarshalMap(res.Item, &p)
	require.Equal(t, Persons{"1", "tom", 100}, p)

	err = d.GetStruct(ctx, persons[0], &p)
	require.NoError(t, err)
	require.Equal(t, Persons{"1", "tom", 100}, p)

	getArg = &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: persons[1].Id},
		},
	}
	getOut, err = d.Get(ctx, getArg)
	res = getOut.(*dynamodb.GetItemOutput)
	require.NoError(t, err)
	require.Equal(t, 0, len(res.Item))

	// drop
	dropArg := &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	}
	_, err = d.DropTable(ctx, dropArg)
	require.NoError(t, err)
}

type Profile struct {
	Id      string `key:"-" dynamodbav:"id"`
	Name    string `dynamodbav:"name"`
	Age     int    `dynamodbav:"age"`
	IconImg string `dynamodbav:"icon-img"`
}

func TestWithS3(t *testing.T) {
	var (
		ctx      = context.Background()
		confOpts = []func(*config.LoadOptions) error{
			config.WithDefaultRegion(TestRegion),
			config.WithSharedConfigProfile(TestProfile),
		}
		profiles = []Profile{
			Profile{"101", "alice", 20, ""},
			Profile{"102", "bob", 50, ""},
		}
		filePaths = []string{TestImagePath, TestImagePath2}
		keys      = []string{TestKey, TestKey2}
	)

	d, err := NewDynamoDB(ctx, confOpts, WithTimeout(5*time.Second))
	require.NoError(t, err)

	s3client, err := s3.NewS3Client(ctx, confOpts, s3.WithTimeout(5*time.Second))
	require.NoError(t, err)

	d.SetS3(&s3client)
	batch := d.WriterWithS3(ctx)

	for i := range profiles {
		file, err := os.Open(filePaths[i])
		require.NoError(t, err)

		err = batch.InsertStructWithS3(ctx, &profiles[i], "IconImg", TestBucketName, keys[i], file)
		require.NoError(t, err)

		file.Close()
	}

	_, err = d.CommitWithS3(ctx, &batch)
	require.NoError(t, err)
}
