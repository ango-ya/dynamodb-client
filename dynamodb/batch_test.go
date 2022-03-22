package dynamodb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ango-ya/aws-s3-client/s3"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	// "github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
)

const (
	TestImagePath  = "./testimg/sample.png"
	TestImagePath2 = "./testimg/sample2.png"
	TestBucketName = "tak-sandbox"
	TestKey        = "sample.png"
	TestKey2       = "sample2.png"
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
	IconImg    string `dynamodbav:"icon-img"`
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
			UpdateExpression: aws.String("SET postal-code = :postal-code, icon-img = :icon-img"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":postal-code": &types.AttributeValueMemberS{Value: addr.PostalCode},
				":icon-img":    &types.AttributeValueMemberS{Value: addr.IconImg},
			},
		},
	}
	require.Equal(t, expected.Update.TableName, b.inputs[0].Update.TableName)
	require.Equal(t, expected.Update.Key, b.inputs[0].Update.Key)
	require.Equal(t, expected.Update.ExpressionAttributeValues, b.inputs[0].Update.ExpressionAttributeValues)
}

func TestUpdateStructWithS3(t *testing.T) {
	file, err := os.Open(TestImagePath)
	require.NoError(t, err)
	defer file.Close()

	var (
		ctx      = context.Background()
		confOpts = []func(*config.LoadOptions) error{
			config.WithDefaultRegion(TestRegion),
			config.WithSharedConfigProfile(TestProfile),
		}
		b    = BatchWriterWithS3{uploadeds: make(map[PairOfBucketNameAndKey]string)}
		addr = AddressBook{
			Id:         1,
			Addr:       "Fukuoka, Japan",
			PostalCode: "123-4567",
		}
		expectedURI = "https://s3.us-east-1.amazonaws.com/tak-sandbox/sample.png"
	)

	s3client, err := s3.NewS3Client(ctx, confOpts, s3.WithTimeout(5*time.Second))
	require.NoError(t, err)

	b.s3client = &s3client

	err = b.UpdateStructWithS3(ctx, &addr, "icon-img", TestBucketName, TestKey, file)
	require.NoError(t, err)

	expectedUploadeds := map[PairOfBucketNameAndKey]string{
		PairOfBucketNameAndKey{Name: TestBucketName, Key: TestKey}: expectedURI,
	}
	require.Equal(t, expectedUploadeds, b.uploadeds)
}
