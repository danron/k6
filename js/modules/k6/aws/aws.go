package aws

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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
