import _set_path  # noqa
import os
from datetime import datetime, timedelta
from finalizeStep import FinalizeStep
from pipelineutilities.pipeline_config import load_cached_config, cache_config
import sentry_sdk as sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(
        dsn=os.environ['SENTRY_DSN'],
        integrations=[AwsLambdaIntegration()]
    )


def run(event, context):
    test_required_fields(event)

    # used to indicate to the choice in the step functions if the step has finished yet
    event['finalize_complete'] = False
    config = load_cached_config(event)

    for id in config.get("ids"):
        if id not in config['finalize_completed_ids']:
            step = FinalizeStep(id, config)
            # step.error = congig.get("unexpected", "")
            # if not step.error:
            #    step.manifest_metadata = json.loads(mu.s3_read_file_content(s3_bucket, s3_schema_path))
            step.run()
            config['finalize_completed_ids'].append(id)

        if break_to_restart_step(config):
            break

    # have we processed all the fields.
    if len(config['ids']) == len(config['finalize_completed_ids']):
        event['finalize_complete'] = True

    if "unexpected" in event:
        config['error_found'] = True
    else:
        config['error_found'] = False

    cache_config(config)

    return event


def test_required_fields(event):
    for key in ['config-file', 'process-bucket']:
        if key not in event:
            raise Exception(key + " required for finalize")


def break_to_restart_step(config):
    return config['finalize_quittime'] <= datetime.utcnow()


def setup_config_for_restarting_step(config):
    config['finalize_quittime'] = datetime.utcnow() + timedelta(seconds=config['seconds-to-allow-for-processing'])

    if 'finalize_completed_ids' not in config:
        config['finalize_completed_ids'] = []

    if 'finalize_run_number' not in config:
        config['finalize_run_number'] = 0

    config['finalize_run_number'] = config['finalize_run_number'] + 1

    if config['finalize_run_number'] > 5:
        raise Exception("Too many executions")


# python -c 'from handler import *; test()'
def test():
    event = {
        'ssm_key_base': '/all/marble-manifest-prod',
        'config-file': '2020-04-15-13:15:10.365990.json',
        'process-bucket': 'marble-manifest-prod-processbucket-13bond538rnnb',
        'errors': []
    }

    print(run(event, {}))
