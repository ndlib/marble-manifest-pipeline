import os
import json
from pathlib import Path
import sys
from helpers import get_file_ids_to_be_processed

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
    if 'ssm_key_base' not in event and not event.get('local', False):
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']

    config = get_pipeline_config(event)

    event['errors'] = []

    if not event.get('ids'):
        all_files = get_matching_s3_objects(config['process-bucket'], config['process-bucket-csv-basepath'] + "/")
        event['ids'] = list(get_file_ids_to_be_processed(all_files, config))

    event['ecs-args'] = [json.dumps(config)]

    return event


# python -c 'from handler import *; test()'
def test():
    data = {}
    data['local'] = True
    data['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
    data['process-bucket-csv-basepath'] = ""
    print(run(data, {}))
