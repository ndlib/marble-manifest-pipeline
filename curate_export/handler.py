# handler.py
""" Module to launch application """

import _set_path  # noqa
import os
import io
import json
# import botocore
from datetime import datetime, timedelta
from pathlib import Path
from curate_api import CurateApi
from read_batch_ingest_combined_csv import read_batch_ingest_combined_csv  # noqa: #402
from pipelineutilities.pipeline_config import setup_pipeline_config, load_config_ssm  # noqa: E402
import sentry_sdk   # noqa: E402
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration  # noqa: E402
from pipelineutilities.s3_helpers import read_s3_json


if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(dsn=os.environ['SENTRY_DSN'], integrations=[AwsLambdaIntegration()])


def run(event: dict, context: dict) -> dict:
    """ Run the process to retrieve and process Aleph metadata. """
    _supplement_event(event)

    config = setup_pipeline_config(event)
    if config:
        time_to_break = datetime.now() + timedelta(seconds=config['seconds-to-allow-for-processing'])
        print("Will break after ", time_to_break)
        curate_config = load_config_ssm(config['curate_keys_ssm_base'])
        config.update(curate_config)
        # if "filenames" in event:
        #     for filename in event["filenames"]:
        #         json_curate_item = read_batch_ingest_combined_csv(filename)
        #         with open(filename + '.json', 'w') as f:
        #             json.dump(json_curate_item, f, indent=2)
        if "ids" not in event:
            event["ids"] = read_ids_from_s3(config['process-bucket'], "source_system_export_ids.json", "Curate")
        # print("event after ids added = ", event)
        if "ids" in event:
            curate_api_class = CurateApi(config, event, time_to_break)
            event["curateHarvestComplete"] = curate_api_class.get_curate_items(event["ids"])
        if event["curate_execution_count"] >= event["max_curate_executions"] and not event["curateHarvestComplete"]:
            event["curateHarvestComplete"] = True
            sentry_sdk.capture_message('Curate did not complete harvest after maximum executions threshold of ' + str(event["max_curate_executions"]))
        if not event['local']:
            event['eadsSavedToS3'] = os.path.join(config['process-bucket'], config['process-bucket-csv-basepath'])
    return event


def _supplement_event(event: dict) -> dict:
    """ Add additional nodes to event if they do not exist. """
    if 'curateHarvestComplete' not in event:
        event['curateHarvestComplete'] = False
    if 'local' not in event:
        event['local'] = False
    if 'ssm_key_base' not in event and 'SSM_KEY_BASE' in os.environ:
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']
    if 'local-path' not in event:
        event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
    event["curate_execution_count"] = event.get("curate_execution_count", 0) + 1
    event["max_curate_executions"] = event.get("max_curate_executions", 5)
    return


def read_ids_from_s3(process_bucket: str, s3_path: str, section: str) -> list:
    ids = []
    try:
        json_hash = read_s3_json(process_bucket, s3_path)
        if section in json_hash:
            ids = json_hash[section]
    except Exception as e:
        sentry_sdk.capture_exception(e)
        print("Control file does not exit:", process_bucket, s3_path)
    return ids


# setup:
# export SSM_KEY_BASE=/all/new-csv
# aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
# python -c 'from handler import *; test()'

# testing:
# python 'run_all_tests.py'
def test(identifier=""):
    """ test exection """
    filename = 'event.json'
    if os.path.exists(filename):
        with io.open(filename, 'r', encoding='utf-8') as json_file:
            event = json.load(json_file)
    else:
        event = {}
        event['local'] = False
        if event['local']:
            # event['seconds-to-allow-for-processing'] = 30
            # und:zp38w953h0s = Commencement Programs
            # und:zp38w953p3c = Chinese Catholic-themed paintings
            # und:qz20sq9094h = Architectural Lantern Slides (huge)
            # ks65h992w12 = Epistemological Letters
            # 1z40ks6792x = Varieties of Democracy - has sub-collections
            # event['ids'] = ["und:1z40ks6792x"]
            event['ids'] = ["und:zp38w953h0s"]
            # event['ids'] = ["und:zp38w953h0s", "und:zp38w953p3c"]

    event = run(event, {})

    if not event['curateHarvestComplete']:
        with open('./event.json', 'w') as f:
            json.dump(event, f, indent=2)
    else:
        try:
            os.remove('./event.json')
        except FileNotFoundError:
            pass
    print(event)
