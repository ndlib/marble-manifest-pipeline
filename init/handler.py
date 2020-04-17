import _set_path  # noqa
import os
from helpers import get_file_ids_to_be_processed, get_all_file_ids

from pipelineutilities.pipeline_config import get_pipeline_config, cache_config, generate_config_filename
from pipelineutilities.s3_helpers import get_matching_s3_objects

import sentry_sdk as sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(
        dsn=os.environ['SENTRY_DSN'],
        integrations=[AwsLambdaIntegration()]
    )


def run(event, context):
    if 'ssm_key_base' not in event and not event.get('local', False):
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']

    event['config-file'] = generate_config_filename()
    event['errors'] = []
    config = get_pipeline_config(event)

    if event.get('ids'):
        config['ids'] = event['ids']
    else:
        all_files = get_matching_s3_objects(config['process-bucket'], config['process-bucket-csv-basepath'] + "/")
        if event.get("run_all_ids", False):
            config['ids'] = list(get_all_file_ids(all_files, config))
        else:
            config['ids'] = list(get_file_ids_to_be_processed(all_files, config))

    cache_config(config, event)

    # reset the event because the data has been moved to config
    event = {
        'config-file': config['config-file'],
        'process-bucket': config['process-bucket'],
        'errors': config['errors'],
        'local': event.get('local', False)
    }
    event['ecs-args'] = [event]

    return event


# python -c 'from handler import *; test()'
def test():
    data = {}
    # data['local'] = True
    # data['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
    # data['process-bucket-csv-basepath'] = ""
    data['ssm_key_base'] = '/all/marble-manifest-prod'
    data['ids'] = [
        '1934.007.001'
    ]
    data['local'] = True

    print(run(data, {}))
