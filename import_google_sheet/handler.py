# handler.py
""" Module to launch application """

import _set_path  # noqa
import os
# import json
from pathlib import Path
from pipelineutilities.pipeline_config import setup_pipeline_config, load_config_ssm  # noqa: E402
import sentry_sdk  # noqa: E402
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration  # noqa: E402
from harvest_google_sheet_info import HarvestGoogleSheetInfo


if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(dsn=os.environ['SENTRY_DSN'], integrations=[AwsLambdaIntegration()])


def run(event, context):
    """ run the process to retrieve and process web kiosk metadata """
    _suplement_event(event)
    config = setup_pipeline_config(event)
    google_config = load_config_ssm(config['google_keys_ssm_base'])
    config.update(google_config)
    harvest_google_sheet_info_class = HarvestGoogleSheetInfo(config)
    google_sheet_id = "1gKUkoG921EW0AAa-9c58Yn3wGyx8UXLnlFCWHs3G7E4"
    sheet_json = harvest_google_sheet_info_class.harvest_google_sheet_info(google_sheet_id)
    print("sheet_json = ", sheet_json)


def _suplement_event(event):
    if 'ssm_key_base' not in event and 'SSM_KEY_BASE' in os.environ:
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']
    if 'local-path' not in event:
        event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
    return


# setup:
# cd museum_export
# aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
# export SSM_KEY_BASE=/all/new-csv
# python -c 'from handler import *; test()'
# python 'run_all_tests.py'
def test():
    """ test exection """
    event = {}
    run(event, {})
