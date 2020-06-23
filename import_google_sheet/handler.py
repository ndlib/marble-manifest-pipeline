# handler.py
""" Module to launch application """

import _set_path  # noqa
import os
import json
import io
from pathlib import Path
from pipelineutilities.pipeline_config import setup_pipeline_config, load_config_ssm  # noqa: E402
import sentry_sdk  # noqa: E402
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration  # noqa: E402
from harvest_google_sheet_info import HarvestGoogleSheetInfo
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
    harvest_google_sheet_info_class = HarvestGoogleSheetInfo(google_credentials)
    local_folder = os.path.dirname(os.path.realpath(__file__)) + '/'
    for site_name in event['sites']:
        filename = local_folder + site_name + '.json'
        try:
            with io.open(filename, 'r', encoding='utf-8') as json_file:
                site_control_json = json.load(json_file)
        except Exception as e:
            print('Unable to open file ' + filename)
            capture_exception(e)
            break
        harvest_google_sheet_info_class.harvest_google_spreadsheet_info(site_name, site_control_json['googleSpreadsheetId'], site_control_json['columnsToExport'], site_control_json['fieldForKey'])
    _save_sites_info_to_s3(local_folder + "sites/", "s3://" + config["process-bucket"] + "/sites/")
    copy_tree(local_folder + "sites/", local_folder + "../process_manifest/sites/")


def _save_sites_info_to_s3(sites_folder, s3_key):
    sync_command = f"aws s3 sync " + sites_folder + " " + s3_key
    os.system(sync_command)


def _suplement_event(event):
    if 'ssm_key_base' not in event and 'SSM_KEY_BASE' in os.environ:
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']
    if 'local-path' not in event:
        event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
    if 'sites' not in event:
        event['sites'] = ['marble']  # assume we always want to automatically capture at least marble site info
    return


# setup:
# cd import_google_sheet
# export SSM_KEY_BASE=/all/new-csv
# python -c 'from handler import *; test()'
# aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
# python 'run_all_tests.py'
def test():
    """ test exection """
    event = {}
    event['sites'] = ['marble']
    run(event, {})
