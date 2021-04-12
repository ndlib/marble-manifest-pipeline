# handler.py
""" Module to launch application """

import _set_path  # noqa
import os
import io
import json
from datetime import datetime, timedelta
from pathlib import Path
from curate_api import CurateApi
from s3_helpers import delete_s3_key, s3_file_exists, write_s3_json
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
        if event.get('curateExecutionCount', 0) == 1 and not event.get('local', True):
            save_source_system_record(config.get('website-metadata-tablename'), 'Curate')
            _save_seed_files_to_s3(config['process-bucket'], 'save')
        if not config.get('local', True):
            curate_config = load_config_ssm(config['curate_keys_ssm_base'])
            config.update(curate_config)
            save_file_system_record(config.get('website-metadata-tablename'), 'Curate', 'Curate')
            if not event.get("ids", False):
                string_list_to_save = _read_harvest_ids_from_json('./source_system_export_ids.json', 'Curate')
                save_harvest_ids(config, 'Curate', string_list_to_save, config.get('website-metadata-tablename'))
                event['ids'] = _read_harvest_ids_from_dynamo(config.get('website-metadata-tablename'), 'Curate')
                event['countToProcess'] = len(event['ids'])

        if "ids" in event:
            print("ids to process: ", event["ids"])
            curate_api_class = CurateApi(config, event, time_to_break)
            event["curateHarvestComplete"] = curate_api_class.process_curate_items(event["ids"])
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


def _read_harvest_ids_from_json(json_file_name: str, section_to_return: str = 'Curate') -> dict:
    """ read local json file into a dictionary, return information related to Curate """
    with open(json_file_name) as json_file:
        data = json.load(json_file)
    return data.get(section_to_return, [])


def _read_harvest_ids_from_dynamo(dynamo_table_name, source_system_name) -> list:
    read_from_dynamo_class = ReadFromDynamo({'local': False}, dynamo_table_name)
    results = read_from_dynamo_class.read_items_to_harvest(source_system_name)
    return results


def _save_seed_files_to_s3(bucket_name, folder_name):
    local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"
    for file_name in os.listdir(folder_name):
        local_file_name = os.path.join(local_folder, folder_name, file_name)
        if os.path.isfile(local_file_name):
            with io.open(local_file_name, 'r', encoding='utf-8') as json_file:
                json_to_save = json.load(json_file)
            s3_key = os.path.join(folder_name, file_name)
            _delete_multipart_s3_file_if_necessary(bucket_name, s3_key)
            print('saving filename to s3 = ', file_name)
            write_s3_json(bucket_name, s3_key, json_to_save)


def _delete_multipart_s3_file_if_necessary(bucket_name, s3_key):
    """ If files were manually uploaded through the aws console and are large (35 meg in my example)
        AWS Console loads then using a multipart upload, whose md5 checksum we cannot accommodate.
        If our file we are checking is such a file, delete it so we can reload it. """
    obj_dict = s3_file_exists(bucket_name, s3_key)
    if not obj_dict:
        return

    etag = (obj_dict['ETag'])
    etag = etag[1:-1]  # strip quotes
    if '-' in etag:
        delete_s3_key(bucket_name, s3_key)


# setup:
# export SSM_KEY_BASE=/all/stacks/steve-manifest
# aws-vault exec testlibnd-superAdmin
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
        event['ids'] = []  # default to read from Dynamo
        event['seconds-to-allow-for-processing'] = 60 * 10 * 5
        # event['local'] = True
        if event['local']:
            # event['seconds-to-allow-for-processing'] = 30
            # und:qz20sq9094h = Architectural Lantern Slides (huge)
            # ks65h992w12 = Epistemological Letters
            # 1z40ks6792x = Varieties of Democracy - has sub-collections
            event['ids'] = ["und:zp38w953h0s"]  # Commencement Programs
            event['ids'] = ["und:zp38w953p3c"]  # Chinese Catholic-themed paintings
            event['ids'] = ["und:n296ww75n6f"]  # Gregorian Archive
        # event['ids'] = ["und:qz20sq9094h"]  # Architectural Lantern Slides (huge)
        event['ids'] = ["und:n296ww75n6f"]  # Gregorian Archive
        event['exportAllFilesFlag'] = True
        event['forceSaveStandardJson'] = True
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
