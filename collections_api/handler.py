"""This module saves a listing of contents of standard json files by source.
    These results are saved into the manifest-server-bucket.
    These are served by calls to cloudfront here:  https://presentation-iiif.library.nd.edu/experimental/collections
    These are used by redbox."""

import _set_path  # noqa
import os
from pipeline_config import setup_pipeline_config
import sentry_sdk as sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
from collections_api import CollectionsApi


if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(
        dsn=os.environ['SENTRY_DSN'],
        integrations=[AwsLambdaIntegration()]
    )


def run(event, context):
    config = setup_pipeline_config(event)
    collections_api_class = CollectionsApi(config)
    collections_api_class.save_collection_details(['aleph', 'archivesspace', 'curate', 'embark'])
    return event


# export SSM_KEY_BASE=/all/new-csv
# aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
# python -c 'from handler import *; test()'
def test():
    event = {}
    event['local'] = False
    if 'ssm_key_base' not in event and 'SSM_KEY_BASE' in os.environ:
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']
    else:
        event['ssm_key_base'] = '/all/new-csv'
    print(run(event, {}))
