import os
import json
from pathlib import Path
import sys
from datetime import datetime, timedelta, timezone
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

    event['ecs-args'] = [json.dumps(config)]
    event['ids'] = []
    event['errors'] = []

    all_files = get_matching_s3_objects(config['process-bucket'], config['process-bucket-csv-basepath'] + "/")
    event['ids'] = list(get_file_ids_to_be_processed(all_files, config))

    return event


def get_file_ids_to_be_processed(files, config):
    time_threshold_for_processing = determine_time_threshold_for_processing(config)
    for file in files:
        # if it is not the basedirectory which is returned in the list and
        # the time is more recent than out test threshold
        if file['Key'] != config['process-bucket-csv-basepath'] + "/" and file['LastModified'] >= time_threshold_for_processing:
            yield get_if_from_file_key(file['Key'])


def determine_time_threshold_for_processing(config):
    time_threshold_for_processing = datetime.utcnow() - timedelta(hours=config['hours-threshold-for-incremental-harvest'])
    # since this is utc already but there is no timezone add it in so
    # the data can be compared to the timze zone aware date in file
    return time_threshold_for_processing.replace(tzinfo=timezone.utc)


def get_if_from_file_key(key):
    # remove the extension
    id = os.path.splitext(key)
    # get the basename (filename)
    return os.path.basename(id[0])


# python -c 'from handler import *; test()'
def test():
    data = {}
    data['local'] = True
    data['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"

    print(run(data, {}))
