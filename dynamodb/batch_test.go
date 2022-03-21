package dynamodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

type User struct {
	Id      string    `dynamodbav:"id"`
	Name    string    `dynamodbav:"name"`
	Age     int       `key:"-" dynamodbav:"age"`
	Created time.Time `dynamodbav:"created"`
}

type AddressBook struct {
	Id         int    `key:"-" dynamodbav:"id"`
	Addr       string `key:"-" dynamodbav:"addr"`
	PostalCode string `dynamodbav:"postal-code"`
}

func TestInsertStruct(t *testing.T) {
	var (
		ctx    = context.Background()
		b      = BatchWriter{}
		now, _ = time.Parse("2006-01-02", "2021-12-31")
		user   = User{
			Id:      "124",
			Name:    "tom",
			Age:     30,
			Created: now,
		}
		err error
	)

	err = b.InsertStruct(ctx, user)
	require.NoError(t, err)

	expected := types.TransactWriteItem{
		Put: &types.Put{
			TableName: aws.String("user"),
			Item: map[string]types.AttributeValue{
				"id":      &types.AttributeValueMemberS{Value: user.Id},
				"name":    &types.AttributeValueMemberS{Value: user.Name},
				"age":     &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", user.Age)},
				"created": &types.AttributeValueMemberS{Value: "2021-12-31T00:00:00Z"},
			},
		},
	}

	require.Equal(t, expected, b.inputs[0])
}

func TestDeleteStruct(t *testing.T) {
	var (
		ctx  = context.Background()
		b    = BatchWriter{}
		user = User{
			Id: "124",
		}
		err error
	)

	err = b.DeleteStruct(ctx, user)
	require.NoError(t, err)

	expected := types.TransactWriteItem{
		Delete: &types.Delete{
			TableName: aws.String("user"),
			Key: map[string]types.AttributeValue{
				"age": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", user.Age)},
			},
		},
	}

	require.Equal(t, expected, b.inputs[0])
}

func TestUpdateStruct(t *testing.T) {
	var (
		ctx  = context.Background()
		b    = BatchWriter{}
		addr = AddressBook{
			Id:         1,
			Addr:       "Fukuoka, Japan",
			PostalCode: "123-4567",
		}
		err error
	)

	err = b.UpdateStruct(ctx, addr)
	require.NoError(t, err)

	expected := types.TransactWriteItem{
		Update: &types.Update{
			TableName: aws.String("address-book"),
			Key: map[string]types.AttributeValue{
				"id":   &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", addr.Id)},
				"addr": &types.AttributeValueMemberS{Value: addr.Addr},
			},
			UpdateExpression: aws.String("SET postal-code = :postal-code"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":postal-code": &types.AttributeValueMemberS{Value: addr.PostalCode},
			},
		},
	}

	require.Equal(t, expected, b.inputs[0])
}
