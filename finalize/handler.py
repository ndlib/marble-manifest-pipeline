import boto3
import os
import sys
import json
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from finalizeStep import finalizeStep

sentry_sdk.init(
    dsn=os.environ['SENTRY_DSN'],
    integrations=[AwsLambdaIntegration()]
)


def run(event, context):
    id = event.get('id')
    s3_bucket = event['process-bucket']
    s3_schema_path = os.path.join(event['process-bucket-read-basepath'], id, event["schema-file"])

    step = finalizeStep(id, event)
    step.error = event.get("unexpected", "")
    if not step.error:
        step.manifest_metadata = json.loads(read_s3_file_content(s3_bucket, s3_schema_path))

    step.run()

    if "unexpected" in event:
        event['error_found'] = True
    else:
        event['error_found'] = False
    return event


def read_s3_file_content(s3_bucket, s3_path):
    content_object = boto3.resource('s3').Object(s3_bucket, s3_path)
    return content_object.get()['Body'].read().decode('utf-8')


# python -c 'from handler import *; test()'
def test():
    with open("../example/example-input.json", 'r') as input_source:
        data = json.load(input_source)
    input_source.close()

    data = {
      "id": "example",
      "data": data
    }
    print(run(data, {}))
