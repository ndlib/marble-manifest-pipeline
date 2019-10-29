import boto3
import json

# state machine to restart
statemachine_arn = "arn:aws:states:us-east-1:333680067100:stateMachine:manifest-pipeline-mets-test-SchemaStateMachine"
# bucket to find process items
bucket = "manifest-pipeline-mets-test-processbucket-uryjoy854u02"

client = boto3.client('stepfunctions')
s3 = boto3.resource('s3')
result = s3.meta.client.list_objects(Bucket=bucket, Prefix="process/", Delimiter='/')

for o in result.get('CommonPrefixes'):
    id = o.get('Prefix').replace("process/", "").replace("/", "")
    print("Starting:", id)
    response = client.start_execution(
        stateMachineArn=statemachine_arn,
        input=json.dumps({"id": id})
    )
