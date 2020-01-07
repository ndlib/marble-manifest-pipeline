import os
import sys
import json
import sentry_sdk
import manifest_utils as mu
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from finalizeStep import FinalizeStep

sentry_sdk.init(
    dsn=os.environ['SENTRY_DSN'],
    integrations=[AwsLambdaIntegration()]
)


def run(event, context):
    id = event.get('id')
    s3_bucket = event['process-bucket']
    s3_schema_path = os.path.join(event['process-bucket-read-basepath'], id, event["schema-file"])

    step = FinalizeStep(id, event)
    step.error = event.get("unexpected", "")
    if not step.error:
        step.manifest_metadata = json.loads(mu.s3_read_file_content(s3_bucket, s3_schema_path))

    step.run()

    if "unexpected" in event:
        event['error_found'] = True
    else:
        event['error_found'] = False
    return event


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
