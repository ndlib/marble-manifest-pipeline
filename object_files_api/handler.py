"""This module used in a lambda and api gateway to send list of files."""

import _set_path  # noqa
import io
import json
import os
from pipeline_config import setup_pipeline_config
import sentry_sdk as sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
from files_api import FilesApi
from pathlib import Path


if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(
        dsn=os.environ['SENTRY_DSN'],
        integrations=[AwsLambdaIntegration()]
    )


def run(event, context):
    _suplement_event(event)
    config = setup_pipeline_config(event)
    files_api_class = FilesApi(event, config)
    directories = files_api_class.save_files_details()
    event['objectFilesApiDirectoriesCount'] = len(directories)
    if event["objectFilesApi_execution_count"] >= event["maximum_objectFilesApi_executions"]:
        event['objectFilesApiComplete'] = True
    return event


def _suplement_event(event):
    if 'local-path' not in event:
        event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
    event['objectFilesApiComplete'] = event.get('objectFilesApiComplete', False)
    event["objectFilesApi_execution_count"] = event.get("objectFilesApi_execution_count", 0) + 1
    event["maximum_objectFilesApi_executions"] = event.get("maximum_objectFilesApi_executions", 10)
    if 'local' not in event:
        event['local'] = False
    return


# export SSM_KEY_BASE=/all/stacks/steve-manifest
# aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --

# aws-vault exec libnd-wse-admin --session-ttl=1h --assume-role-ttl=1h --
# python -c 'from handler import *; test()'
def test():
    filename = 'event.json'
    if os.path.exists(filename):
        with io.open(filename, 'r', encoding='utf-8') as json_file:
            event = json.load(json_file)
    else:
        event = {}
        event["local"] = False
        event['ssm_key_base'] = '/all/stacks/steve-manifest'
        event['seconds-to-allow-for-processing'] = 9000
        # event['ssm_key_base'] = '/all/marble-manifest-prod'
        # event['rbsc-image-bucket'] = 'libnd-smb-rbsc'
        # event['image-server-base-url'] = 'http://images.com'
        # event['image-server-bucket'] = 's3://images'
        # event['manifest-server-bucket'] = 'steve-manifest-manifestbucket46c412a5-19kyrt97zbq12'
    event = run(event, {})

    if not event['objectFilesApiComplete']:
        with open('event.json', 'w') as output_file:
            json.dump(event, output_file, indent=2)
    else:
        try:
            os.remove('event.json')
        except FileNotFoundError:
            pass
    print(event)
