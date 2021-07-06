# handler.py
""" Module to launch ArchivesSpace Export application """
import _set_path  # noqa
import io
import json
import os
import time
from datetime import datetime, timedelta
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration  # noqa: E402
from harvest_oai_eads import HarvestOaiEads  # noqa: #502
from pipelineutilities.pipeline_config import setup_pipeline_config  # noqa: E402
from pipelineutilities.standard_json_helpers import StandardJsonHelpers
from pipelineutilities.save_standard_json import save_standard_json
from pipelineutilities.save_standard_json_to_dynamo import SaveStandardJsonToDynamo
from dynamo_save_functions import save_source_system_record, save_harvest_ids
from read_from_dynamo import ReadFromDynamo


def run(event: dict, _context: dict):
    """ Run the process to retrieve and process ArchivesSpace metadata.

    Information on the API can be found here:
        http://archivesspace.github.io/archivesspace/api/ """
    _supplement_event(event)
    _init_sentry()
    config = setup_pipeline_config(event)
    if not event.get("ids", False):
        string_list_to_save = _read_harvest_ids_from_json('./source_system_export_ids.json')
        save_harvest_ids(config, 'ArchivesSpace', string_list_to_save, config.get('website-metadata-tablename'))
        event['ids'] = _read_harvest_ids_from_dynamo(config.get('website-metadata-tablename'), 'ArchivesSpace')
        event['countToProcess'] = len(event['ids'])
    start_time = time.time()
    time_to_break = datetime.now() + timedelta(seconds=config['seconds-to-allow-for-processing'])
    print("Will break after ", time_to_break)
    if event.get('archivesSpaceExecutionCount', 0) == 1 and not event.get('local'):
        save_source_system_record(config.get('website-metadata-tablename'), 'ArchivesSpace')
    harvest_oai_eads_class = HarvestOaiEads(config)
    standard_json_helpers_class = StandardJsonHelpers(config)
    save_standard_json_to_dynamo_class = SaveStandardJsonToDynamo(config)
    ids = event.get("ids", [])
    while len(ids) > 0 and datetime.now() < time_to_break:
        standard_json = harvest_oai_eads_class.get_standard_json_from_archives_space_url(ids[0])
        if standard_json:
            print("ArchivesSpace ead_id = ", standard_json.get("id", ""), " source_system_url = ", ids[0], int(time.time() - start_time), 'seconds.')
            standard_json = standard_json_helpers_class.enhance_standard_json(standard_json)
            save_standard_json(config, standard_json)
            save_standard_json_to_dynamo_class.save_standard_json(standard_json)
        del ids[0]
    event['countRemaining'] = len(event['ids'])
    event['archivesSpaceHarvestComplete'] = (len(ids) == 0)
    event['eadsSavedToS3'] = os.path.join(config['process-bucket'], config['process-bucket-data-basepath'])
    if event["archivesSpaceExecutionCount"] >= event["maximumArchivesSpaceExecutions"]:
        event['archivesSpaceHarvestComplete'] = True
    return event


def _supplement_event(event: dict) -> dict:
    if 'archivesSpaceHarvestComplete' not in event:
        event['archivesSpaceHarvestComplete'] = False
    if 'ssm_key_base' not in event and 'SSM_KEY_BASE' in os.environ:
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']
    event['archivesSpaceExecutionCount'] = event.get('archivesSpaceExecutionCount', 0) + 1
    event['maximumArchivesSpaceExecutions'] = 10
    return event


def _read_harvest_ids_from_json(json_file_name: str) -> dict:
    """ read local json file into a dictionary, return information related to ArchivesSpace """
    with open(json_file_name) as json_file:
        data = json.load(json_file)
    return data['ArchivesSpace']


def _read_harvest_ids_from_dynamo(dynamo_table_name, source_system_name) -> list:
    read_from_dynamo_class = ReadFromDynamo({'local': False}, dynamo_table_name)
    results = read_from_dynamo_class.read_items_to_harvest(source_system_name)
    return results


def _init_sentry():
    if 'SENTRY_DSN' in os.environ:
        sentry_sdk.init(dsn=os.environ['SENTRY_DSN'], integrations=[AwsLambdaIntegration()])


# setup:
# export SSM_KEY_BASE=/all/stacks/steve-manifest
# aws-vault exec testlibnd-superAdmin
# python -c 'from handler import *; test()'
def test():
    """ test exection """
    filename = 'event.json'
    if os.path.exists(filename):
        with io.open(filename, 'r', encoding='utf-8') as json_file:
            event = json.load(json_file)
    else:
        event = {}
        event["local"] = False
        event['seconds-to-allow-for-processing'] = 60 * 10 * 5
        event["ids"] = [
            # "https://archivesspace.library.nd.edu/repositories/2/resources/1652",  # Collegiate Jazz Festival - These contain PDFs
            # "https://archivesspace.library.nd.edu/repositories/3/resources/1631",    # Inquisitions (MSHLAT0090_EAD)
            # "https://archivesspace.library.nd.edu/repositories/3/resources/1447",
            # "https://archivesspace.library.nd.edu/repositories/3/resources/1567",
            # "https://archivesspace.library.nd.edu/repositories/3/resources/1644",  # Irish Broadsides
        ]
        # event["ids"] = ["https://archivesspace.library.nd.edu/repositories/3/resources/1492"]  # Parsons Journals
        # event["ids"] = ["https://archivesspace.library.nd.edu/repositories/3/resources/1524"]
        # event['ids'] = ["https://archivesspace.library.nd.edu/repositories/3/resources/2038"]
        # event['ids'] = ["https://archivesspace.library.nd.edu/repositories/3/resources/1567"]
        # event['ids'] = ["https://archivesspace.library.nd.edu/repositories/3/resources/1439"]
        # event['ids'] = ["https://archivesspace.library.nd.edu/repositories/3/resources/1644"]  # Irish Broadsides
        # event['ids'] = ["https://archivesspace.library.nd.edu/repositories/2/resources/1652"]  # Collegiate Jazz Festival - These contain PDFs
        # event['ids'] = ["https://archivesspace.library.nd.edu/repositories/2/resources/1652", "https://archivesspace.library.nd.edu/repositories/3/resources/1479"]
        event['exportAllFilesFlag'] = True
        event['forceSaveStandardJson'] = True
    event = run(event, {})

    if not event['archivesSpaceHarvestComplete']:
        with open('event.json', 'w') as json_file:
            json.dump(event, json_file, indent=2)
    else:
        try:
            os.remove('event.json')
        except FileNotFoundError:
            pass
    print(event)
