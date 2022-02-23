package dynamodb

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

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

type Person struct {
	Id   string
	Name string
	Age  int
}

func TestAll(t *testing.T) {
	var (
		ctx       = context.Background()
		tableName = "persons"
		confOpts  = []func(*config.LoadOptions) error{
			config.WithDefaultRegion(TestRegion),
			config.WithSharedConfigProfile(TestProfile),
		}
		persons = []Person{
			Person{"1", "tom", 30},
			Person{"2", "tim", 40},
			Person{"3", "tik", 50},
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
	batch, _ := d.Writer(ctx)
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
	listOut, err := d.List(ctx, listArg)
	require.NoError(t, err)

	resp := listOut.(*dynamodb.ScanOutput)
	require.Equal(t, len(persons), int(resp.ScannedCount))

	var result []Person
	err = attributevalue.UnmarshalListOfMaps(resp.Items, &result)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal Dynamodb Scan Items, %v", err))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Id < result[j].Id
	})
	require.EqualValues(t, persons, result)

	// update & delete
	batch, _ = d.Writer(ctx)
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

	var p Person
	err = attributevalue.UnmarshalMap(res.Item, &p)
	require.Equal(t, Person{"1", "tom", 100}, p)

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
