package aws

import (
	"testing"

	. "github.com/qiniu/logkit/utils/models"

	"github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
)

var log1 = `{"Records": [{
    "eventVersion": "1.0",
    "userIdentity": {
        "type": "IAMUser",
        "principalId": "EX_PRINCIPAL_ID",
        "arn": "arn:aws:iam::123456789012:user/Alice",
        "accountId": "123456789012",
        "accessKeyId": "EXAMPLE_KEY_ID",
        "userName": "Alice",
        "sessionContext": {"attributes": {
            "mfaAuthenticated": "false",
            "creationDate": "2014-03-06T15:15:06Z"
        }}
    },
    "eventTime": "2014-03-06T17:10:34Z",
    "eventSource": "ec2.amazonaws.com",
    "eventName": "CreateKeyPair",
    "awsRegion": "us-east-2",
    "sourceIPAddress": "72.21.198.64",
    "userAgent": "EC2ConsoleBackend, aws-sdk-java/Linux/x.xx.fleetxen Java_HotSpot(TM)_64-Bit_Server_VM/xx",
    "requestParameters": {"keyName": "mykeypair"},
    "responseElements": {
        "keyName": "mykeypair",
        "keyFingerprint": "30:1d:46:d0:5b:ad:7e:1b:b6:70:62:8b:ff:38:b5:e9:ab:5d:b8:21",
        "keyMaterial": "\u003csensitiveDataRemoved\u003e"
    }
}],"Haha":"nihao"}`

var log1exp = `{
    "eventVersion": "1.0",
    "userIdentity": {
        "type": "IAMUser",
        "principalId": "EX_PRINCIPAL_ID",
        "arn": "arn:aws:iam::123456789012:user/Alice",
        "accountId": "123456789012",
        "accessKeyId": "EXAMPLE_KEY_ID",
        "userName": "Alice",
        "sessionContext": {"attributes": {
            "mfaAuthenticated": "false",
            "creationDate": "2014-03-06T15:15:06Z"
        }}
    },
    "eventTime": "2014-03-06T17:10:34Z",
    "eventSource": "ec2.amazonaws.com",
    "eventName": "CreateKeyPair",
    "awsRegion": "us-east-2",
    "sourceIPAddress": "72.21.198.64",
    "userAgent": "EC2ConsoleBackend, aws-sdk-java/Linux/x.xx.fleetxen Java_HotSpot(TM)_64-Bit_Server_VM/xx",
    "requestParameters": {"keyName": "mykeypair"},
    "responseElements": {
        "keyName": "mykeypair",
        "keyFingerprint": "30:1d:46:d0:5b:ad:7e:1b:b6:70:62:8b:ff:38:b5:e9:ab:5d:b8:21",
        "keyMaterial": "\u003csensitiveDataRemoved\u003e"
    },
	"Haha":"nihao"
}`

var log2 = `{"Records": [{
    "eventVersion": "1.04",
    "userIdentity": {
        "type": "IAMUser",
        "principalId": "EX_PRINCIPAL_ID",
        "arn": "arn:aws:iam::123456789012:user/Alice",
        "accountId": "123456789012",
        "accessKeyId": "EXAMPLE_KEY_ID",
        "userName": "Alice"
    },
    "eventTime": "2016-07-14T19:15:45Z",
    "eventSource": "cloudtrail.amazonaws.com",
    "eventName": "UpdateTrail",
    "awsRegion": "us-east-2",
    "sourceIPAddress": "205.251.233.182",
    "userAgent": "aws-cli/1.10.32 Python/2.7.9 Windows/7 botocore/1.4.22",
    "errorCode": "TrailNotFoundException",
    "errorMessage": "Unknown trail: myTrail2 for the user: 123456789012",
    "requestParameters": {"name": "myTrail2"},
    "responseElements": null,
    "requestID": "5d40662a-49f7-11e6-97e4-d9cb6ff7d6a3",
    "eventID": "b7d4398e-b2f0-4faa-9c76-e2d316a8d67f",
    "eventType": "AwsApiCall",
    "recipientAccountId": "123456789012"
},{
    "eventVersion": "1.0",
    "userIdentity": {
        "type": "IAMUser",
        "principalId": "EX_PRINCIPAL_ID",
        "arn": "arn:aws:iam::123456789012:user/Alice",
        "accountId": "123456789012",
        "accessKeyId": "EXAMPLE_KEY_ID",
        "userName": "Alice"
    },
    "eventTime": "2014-03-24T21:11:59Z",
    "eventSource": "iam.amazonaws.com",
    "eventName": "CreateUser",
    "awsRegion": "us-east-2",
    "sourceIPAddress": "127.0.0.1",
    "userAgent": "aws-cli/1.3.2 Python/2.7.5 Windows/7",
    "requestParameters": {"userName": "Bob"},
    "responseElements": {"user": {
        "createDate": "Mar 24, 2014 9:11:59 PM",
        "userName": "Bob",
        "arn": "arn:aws:iam::123456789012:user/Bob",
        "path": "/",
        "userId": "EXAMPLEUSERID"
    }}
}]}`

var log2exp1 = `{
    "eventVersion": "1.04",
    "userIdentity": {
        "type": "IAMUser",
        "principalId": "EX_PRINCIPAL_ID",
        "arn": "arn:aws:iam::123456789012:user/Alice",
        "accountId": "123456789012",
        "accessKeyId": "EXAMPLE_KEY_ID",
        "userName": "Alice"
    },
    "eventTime": "2016-07-14T19:15:45Z",
    "eventSource": "cloudtrail.amazonaws.com",
    "eventName": "UpdateTrail",
    "awsRegion": "us-east-2",
    "sourceIPAddress": "205.251.233.182",
    "userAgent": "aws-cli/1.10.32 Python/2.7.9 Windows/7 botocore/1.4.22",
    "errorCode": "TrailNotFoundException",
    "errorMessage": "Unknown trail: myTrail2 for the user: 123456789012",
    "requestParameters": {"name": "myTrail2"},
    "responseElements": null,
    "requestID": "5d40662a-49f7-11e6-97e4-d9cb6ff7d6a3",
    "eventID": "b7d4398e-b2f0-4faa-9c76-e2d316a8d67f",
    "eventType": "AwsApiCall",
    "recipientAccountId": "123456789012"
}`
var log2exp2 = `{
    "eventVersion": "1.0",
    "userIdentity": {
        "type": "IAMUser",
        "principalId": "EX_PRINCIPAL_ID",
        "arn": "arn:aws:iam::123456789012:user/Alice",
        "accountId": "123456789012",
        "accessKeyId": "EXAMPLE_KEY_ID",
        "userName": "Alice"
    },
    "eventTime": "2014-03-24T21:11:59Z",
    "eventSource": "iam.amazonaws.com",
    "eventName": "CreateUser",
    "awsRegion": "us-east-2",
    "sourceIPAddress": "127.0.0.1",
    "userAgent": "aws-cli/1.3.2 Python/2.7.5 Windows/7",
    "requestParameters": {"userName": "Bob"},
    "responseElements": {"user": {
        "createDate": "Mar 24, 2014 9:11:59 PM",
        "userName": "Bob",
        "arn": "arn:aws:iam::123456789012:user/Bob",
        "path": "/",
        "userId": "EXAMPLEUSERID"
    }}
}`

func TestClocktrailTransformer(t *testing.T) {
	ct := &CloudTrail{}
	var data1, data1exp Data
	err := jsoniter.Unmarshal([]byte(log1), &data1)
	assert.NoError(t, err)

	gotdata, err := ct.Transform([]Data{data1})
	assert.NoError(t, err)

	err = jsoniter.Unmarshal([]byte(log1exp), &data1exp)
	assert.NoError(t, err)

	assert.Equal(t, []Data{data1exp}, gotdata)

	var data2, data2exp1, data2exp2 Data
	err = jsoniter.Unmarshal([]byte(log2), &data2)
	assert.NoError(t, err)

	gotdata, err = ct.Transform([]Data{data2})
	assert.NoError(t, err)

	err = jsoniter.Unmarshal([]byte(log2exp1), &data2exp1)
	assert.NoError(t, err)

	err = jsoniter.Unmarshal([]byte(log2exp2), &data2exp2)
	assert.NoError(t, err)

	assert.Equal(t, []Data{data2exp1, data2exp2}, gotdata)
}
