# edit the state machine you want to run and the bucket below.
# future could read this out of ssm
import boto3
import json
import time

# state machine to restart
statemachine_arn = "arn:aws:states:us-east-1:230391840102:stateMachine:marble-manifest-prod-SchemaStateMachine"
# bucket to find process items
bucket = "marble-manifest-prod-processbucket-kskqchthxshg"

client = boto3.client('stepfunctions')
s3 = boto3.resource('s3')
result = s3.meta.client.list_objects(Bucket=bucket, Prefix="process/", Delimiter='/')
count = 1

for o in result.get('CommonPrefixes'):
    id = o.get('Prefix').replace("process/", "").replace("/", "")
    print("Starting:", id)
    if count % 5 == 0:
        print("sleeping")
        time.sleep(150)

    count += 1

    response = client.start_execution(
        stateMachineArn=statemachine_arn,
        input=json.dumps({"id": id})
    )
