# handler.py
""" Module to launch application """

import _set_path  # noqa
import os
import json
from pathlib import Path
from pipelineutilities.pipeline_config import setup_pipeline_config, load_config_ssm  # noqa: E402
import sentry_sdk  # noqa: E402
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration  # noqa: E402
from harvest_metadata_rules import HarvestMetadataRules
from dependencies.sentry_sdk import capture_exception
from distutils.dir_util import copy_tree


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
    _save_sites_info_to_s3(local_folder + "sites/", "s3://" + config["process-bucket"] + "/sites/")
    try:
        copy_tree(local_folder + "sites/", local_folder + "../process_manifest/sites/")
    except Exception:
        print('Unable to sync sites files to process_manifest ')
        pass


def _save_sites_info_to_s3(sites_folder, s3_key):
    try:
        sync_command = f"aws s3 sync " + sites_folder + " " + s3_key
        os.system(sync_command)
    except Exception as e:
        print('Unable to sync sites files (' + sites_folder + ') to the process bucket here:', s3_key)
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
# python 'run_all_tests.py'
def test():
    """ test exection """
    event = {}
    event['sites'] = ['marble']
    run(event, {})
