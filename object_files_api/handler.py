"""This module used in a lambda and api gateway to send list of files."""

import _set_path  # noqa
import os
from pipeline_config import setup_pipeline_config
import sentry_sdk as sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
from files_api import FilesApi

if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(
        dsn=os.environ['SENTRY_DSN'],
        integrations=[AwsLambdaIntegration()]
    )


def run(event, context):
    config = setup_pipeline_config(event)
    files_api_class = FilesApi(event, config)
    directories = files_api_class.save_files_details()
    event['objectFilesApiDirectoriesCount'] = len(directories)
    return event


# aws-vault exec libnd-wse-admin --session-ttl=1h --assume-role-ttl=1h --
# python -c 'from handler import *; test()'
def test():
    event = {}
    event['local'] = False
    event['ssm_key_base'] = '/all/new-csv'
    # event['ssm_key_base'] = '/all/marble-manifest-prod'
    # event['rbsc-image-bucket'] = 'libnd-smb-rbsc'
    event['image-server-base-url'] = 'http://images.com'
    event['image-server-bucket'] = 's3://images'
    event['manifest-server-bucket'] = 'steve-manifest-manifestbucket46c412a5-19kyrt97zbq12'
    run(event, {})
