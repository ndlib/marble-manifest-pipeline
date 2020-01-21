import os
import sys
import json
from finalizeStep import FinalizeStep
where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am)
sys.path.append(where_i_am + "/dependencies")

from dependencies.pipelineutilities.pipeline_config import get_pipeline_config
from dependencies.pipelineutilities.s3_helpers import get_matching_s3_objects

import dependencies.sentry_sdk as sentry_sdk
from dependencies.sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

sentry_sdk.init(
    dsn=os.environ['SENTRY_DSN'],
    integrations=[AwsLambdaIntegration()]
)


def run(event, context):
    # s3_bucket = event['process-bucket']
    # s3_schema_path = os.path.join(event['process-bucket-read-basepath'], id, event["schema-file"])
    if 'ssm_key_base' not in event:
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']

    config = get_pipeline_config(event)

    ids = event.get("ids")
    for id in ids:

        step = FinalizeStep(id, config)
        # step.error = event.get("unexpected", "")
        # if not step.error:
        #    step.manifest_metadata = json.loads(mu.s3_read_file_content(s3_bucket, s3_schema_path))
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
