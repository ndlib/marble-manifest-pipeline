import _set_path  # noqa
import os
from finalizeStep import FinalizeStep
from pipelineutilities.pipeline_config import load_cached_config, cache_config
import sentry_sdk as sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
from datetime import datetime, timedelta

if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(
        dsn=os.environ['SENTRY_DSN'],
        integrations=[AwsLambdaIntegration()]
    )


def run(event, context):
    # required keys
    for key in ['config-file', 'process-bucket']:
        if key not in event:
            raise Exception(key + " required for finalize")

    config = load_cached_config(event)
    ids = config.get("ids")

    quittime = datetime.utcnow() + timedelta(seconds=config['seconds-to-allow-for-processing'])
    config['finalize_complete'] = False

    if 'finished_ids' not in config:
        config['finished_ids'] = []

    if 'finalized_run_number' not in config:
        config['finalized_run_number'] = 0
    config['finalized_run_number'] = config['finalized_run_number'] + 1

    if config['finalized_run_number'] > 5:
        raise Exception("Too many executions")

    for id in ids:
        if id not in config['finished_ids']:
            step = FinalizeStep(id, config)
            # step.error = congig.get("unexpected", "")
            # if not step.error:
            #    step.manifest_metadata = json.loads(mu.s3_read_file_content(s3_bucket, s3_schema_path))
            step.run()
            config['finished_ids'].append(id)

        if quittime <= datetime.utcnow():
            break

    if len(config['ids']) == len(config['finished_ids']):
        config['finalize_complete'] = True

    if "unexpected" in event:
        config['error_found'] = True
    else:
        config['error_found'] = False

    cache_config(config)
    return event


# python -c 'from handler import *; test()'
def test():
    event = {
        'ssm_key_base': '/all/marble-manifest-prod',
        'config-file': '2020-04-15-13:15:10.365990.json',
        'process-bucket': 'marble-manifest-prod-processbucket-13bond538rnnb',
        'errors': []
    }

    print(run(event, {}))
