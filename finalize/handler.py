import os
import sys
import time
from pathlib import Path
from finalizeStep import FinalizeStep
where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am)
sys.path.append(where_i_am + "/dependencies")

from dependencies.pipelineutilities.pipeline_config import get_pipeline_config

import dependencies.sentry_sdk as sentry_sdk
from dependencies.sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
from datetime import datetime, timedelta, timezone

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

    quittime = datetime.utcnow() + timedelta(seconds=config['seconds-to-allow-for-processing'])
    event['finalize_complete'] = False

    if 'finished_ids' not in event:
        event['finished_ids'] = []

    if 'finalized_run_number' not in event:
        event['finalized_run_number'] = 0
    event['finalized_run_number'] = event['finalized_run_number'] + 1

    if event['finalized_run_number'] > 5:
        raise("Too many executrions")

    for id in ids:
        if id not in event['finished_ids']:
            step = FinalizeStep(id, config)
            # step.error = event.get("unexpected", "")
            # if not step.error:
            #    step.manifest_metadata = json.loads(mu.s3_read_file_content(s3_bucket, s3_schema_path))
            step.run()
            event['finished_ids'].append(id)

        if quittime <= datetime.utcnow():
            break

    if len(config['ids']) == len(event['finished_ids']):
        event['finalize_complete'] = True

    if "unexpected" in event:
        event['error_found'] = True
    else:
        event['error_found'] = False

    return event


# python -c 'from handler import *; test()'
def test():
    data = {
      "ids": ["parsons"],
      "local": True,
      "ssm_key_base": "ds",
      "local-path": str(Path(__file__).parent.absolute()) + "/../example/"
    }
    print(run(data, {}))
