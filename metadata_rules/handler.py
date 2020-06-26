# handler.py
""" Module to launch application """

import _set_path  # noqa
import os
import json
from pathlib import Path
from pipelineutilities.pipeline_config import setup_pipeline_config, load_config_ssm
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
from harvest_metadata_rules import HarvestMetadataRules
from dependencies.sentry_sdk import capture_exception
from distutils.dir_util import copy_tree
from pipelineutilities.s3_sync import s3_sync


if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(dsn=os.environ['SENTRY_DSN'], integrations=[AwsLambdaIntegration()])


def run(event, context):
    """ run the process to retrieve and process web kiosk metadata """
    _suplement_event(event)
    config = setup_pipeline_config(event)
    google_config = load_config_ssm(config['google_keys_ssm_base'])
    config.update(google_config)
    google_credentials = json.loads(config["museum-google-credentials"])
    harvest_metadata_rules_class = HarvestMetadataRules(google_credentials)
    local_folder = os.path.dirname(os.path.realpath(__file__)) + '/'
    for site_name in event['sites']:
        harvest_metadata_rules_class.harvest_google_spreadsheet_info(site_name)
    s3_sync(config["process-bucket"], "sites", local_folder + "sites")
    try:
        copy_tree(local_folder + "sites/", local_folder + "../process_manifest/sites/")
    except EnvironmentError as e:
        print('Unable to sync sites files to process_manifest ')
        capture_exception(e)


def _suplement_event(event):
    if 'ssm_key_base' not in event and 'SSM_KEY_BASE' in os.environ:
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']
    if 'local-path' not in event:
        event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
    if 'sites' not in event:
        event['sites'] = ['marble']  # assume we always want to automatically capture at least marble site info
    return


# setup:
# cd metadata_rules
# export SSM_KEY_BASE=/all/new-csv
# aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
# python -c 'from handler import *; test()'
def test():
    """ test exection """
    event = {}
    event['sites'] = ['marble']
    run(event, {})
