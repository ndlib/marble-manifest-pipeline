# handler.py
""" Module to launch application """

import _set_path  # noqa
import os
import io
import json
from datetime import datetime, timedelta
from pathlib import Path
from curate_api import CurateApi
# from read_batch_ingest_combined_csv import read_batch_ingest_combined_csv
from pipelineutilities.pipeline_config import setup_pipeline_config, load_config_ssm
import sentry_sdk   # noqa: E402
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
from dynamo_save_functions import save_source_system_record, save_harvest_ids, save_file_system_record
from read_from_dynamo import ReadFromDynamo


if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(dsn=os.environ['SENTRY_DSN'], integrations=[AwsLambdaIntegration()])


def run(event: dict, context: dict) -> dict:
    """ Run the process to retrieve and process Aleph metadata. """
    _supplement_event(event)

    config = setup_pipeline_config(event)
    if config:
        time_to_break = datetime.now() + timedelta(seconds=config['seconds-to-allow-for-processing'])
        print("Will break after ", time_to_break)
        if event.get('curateExecutionCount', 0) == 1 and not event.get('local'):
            save_source_system_record(config.get('website-metadata-tablename'), 'Curate')
        curate_config = load_config_ssm(config['curate_keys_ssm_base'])
        config.update(curate_config)
        save_file_system_record(config.get('website-metadata-tablename'), 'Curate', 'Curate')
        if not event.get("ids", False):
            string_list_to_save = _read_harvest_ids_from_json('./source_system_export_ids.json')
            save_harvest_ids(config, 'Curate', string_list_to_save, config.get('website-metadata-tablename'))
            event['ids'] = _read_harvest_ids_from_dynamo(config.get('website-metadata-tablename'), 'Curate')
            event['countToProcess'] = len(event['ids'])

        if "ids" in event:
            print("ids to process: ", event["ids"])
            curate_api_class = CurateApi(config, event, time_to_break)
            event["curateHarvestComplete"] = curate_api_class.get_curate_items(event["ids"])
        event['countRemaining'] = len(event['ids'])
        if event["curateExecutionCount"] >= event["maxCurateExecutions"] and not event["curateHarvestComplete"]:
            event["curateHarvestComplete"] = True
            sentry_sdk.capture_message('Curate did not complete harvest after maximum executions threshold of ' + str(event["maxCurateExecutions"]))
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
    event["curateExecutionCount"] = event.get("curateExecutionCount", 0) + 1
    event["maxCurateExecutions"] = event.get("maxCurateExecutions", 10)
    return


def _read_harvest_ids_from_json(json_file_name: str) -> dict:
    """ read local json file into a dictionary, return information related to Curate """
    with open(json_file_name) as json_file:
        data = json.load(json_file)
    return data['Curate']


def _read_harvest_ids_from_dynamo(dynamo_table_name, source_system_name) -> list:
    read_from_dynamo_class = ReadFromDynamo({'local': False}, dynamo_table_name)
    results = read_from_dynamo_class.read_items_to_harvest(source_system_name)
    return results


# setup:
# export SSM_KEY_BASE=/all/stacks/steve-manifest
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
        event['seconds-to-allow-for-processing'] = 9000
        if event['local']:
            # event['seconds-to-allow-for-processing'] = 30
            # und:qz20sq9094h = Architectural Lantern Slides (huge)
            # ks65h992w12 = Epistemological Letters
            # 1z40ks6792x = Varieties of Democracy - has sub-collections
            event['ids'] = ["und:zp38w953h0s"]  # Commencement Programs
            event['ids'] = ["und:zp38w953p3c"]  # Chinese Catholic-themed paintings
            event['ids'] = ["und:n296ww75n6f"]  # Gregorian Archive
        event['ids'] = []  # force read from Dynamo
        event['export_all_files_flag'] = True  # test exporting all files needing processing
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
