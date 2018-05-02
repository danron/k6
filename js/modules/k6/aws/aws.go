package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/dop251/goja"
	"github.com/loadimpact/k6/js/common"
)

type AWS struct {
	session  *session.Session
	dynamodb *dynamodb.DynamoDB
}

/*
* Create a new AWS struct
 */
func New() *AWS {
	return &AWS{}
}

/*
* Set AWS region.
 */
func (_aws *AWS) SetRegion(ctx context.Context, region goja.Value) bool {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region.String()),
	})
	if err != nil {
		return false
	}
	_aws.session = sess
	_aws.dynamodb = dynamodb.New(sess)
	return true
}

/*
*
 */
func (_aws *AWS) CreateTable(ctx context.Context, tablename goja.Value, tabledef goja.Value) string {

	//svc := dynamodb.New(_aws.session)

	rt := common.GetRuntime(ctx)
	//state := common.GetState(ctx)
	obj := tabledef.ToObject(rt)

	keys := obj.Keys()
	attributes := make([]*dynamodb.AttributeDefinition, len(keys))
	keyschemas := make([]*dynamodb.KeySchemaElement, len(keys))

	for i, key := range obj.Keys() {
		val := obj.Get(key).String()

		attributes[i] = &dynamodb.AttributeDefinition{
			AttributeName: aws.String(key),
			AttributeType: aws.String(val),
		}

		var keyType string
		if i == 0 {
			keyType = "HASH"
		} else {
			keyType = "RANGE"
		}

		keyschemas[i] = &dynamodb.KeySchemaElement{
			AttributeName: aws.String(key),
			KeyType:       aws.String(keyType),
		}
	}

	provisionThroughput := &dynamodb.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64(10),
		WriteCapacityUnits: aws.Int64(10),
	}

	tableName := tablename.String()
	createarg := &dynamodb.CreateTableInput{
		AttributeDefinitions:  attributes,
		KeySchema:             keyschemas,
		TableName:             &tableName,
		ProvisionedThroughput: provisionThroughput,
	}

	_, err := _aws.dynamodb.CreateTable(createarg)
	if err != nil {
		return err.Error()
	}

	return "Ok"
}

/*
*
 */
func (_aws *AWS) PutItem(ctx context.Context, tablename goja.Value, item goja.Value) string {
	av, err := dynamodbattribute.MarshalMap(item.Export())
	if err != nil {
		fmt.Println("Got error marshalling map:")
		fmt.Println(err.Error())
		return "ERROR"
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tablename.String()),
	}

	s, err := _aws.dynamodb.PutItem(input)

	if err != nil {
		fmt.Println("Got error calling PutItem:")
		fmt.Println(err.Error())
		return "ERROR"
	}
	return s.GoString()
}

/*
*
 */
func (_aws *AWS) DeleteItem(ctx context.Context, tablename goja.Value, item goja.Value) string {
	av, err := dynamodbattribute.MarshalMap(item.Export())
	if err != nil {
		fmt.Println("Got error marshalling map:")
		fmt.Println(err.Error())
		return "ERROR"
	}

	input := &dynamodb.DeleteItemInput{
		Key:       av,
		TableName: aws.String(tablename.String()),
	}

	_, err = _aws.dynamodb.DeleteItem(input)

	if err != nil {
		fmt.Println("Got error calling DeleteItem")
		fmt.Println(err.Error())
		return "ERROR"
	}

	return "OK"
}

/*
*
 */
func (_aws *AWS) GetItem(ctx context.Context, tablename goja.Value, query goja.Value) *map[string]interface{} {
	av, err := dynamodbattribute.MarshalMap(query.Export())
	if err != nil {
		fmt.Println("Got error marshalling map:")
		fmt.Println(err.Error())
		return nil
	}
	result, err := _aws.dynamodb.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(tablename.String()),
		Key:       av,
	})

	if err != nil {
		fmt.Println("Error calling GetItem")
		fmt.Println(err.Error())
		return nil
	}

	item := make(map[string]interface{})
	err = dynamodbattribute.UnmarshalMap(result.Item, &item)

	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal Record, %v", err))
	}

	return &item
}
