# handler.py
""" Module to launch application """
import _set_path  # noqa
import json
import io
import os
import time
from datetime import datetime, timedelta
from harvest_oai_eads import HarvestOaiEads  # noqa: #502
from pipelineutilities.pipeline_config import setup_pipeline_config  # noqa: E402
import sentry_sdk as sentry_sdk  # noqa: E402
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration  # noqa: E402
from pipelineutilities.s3_helpers import write_s3_json, write_s3_file
from convert_json_to_csv import ConvertJsonToCsv
from pipelineutilities.search_files import id_from_url, crawl_available_files  # noqa: #402
from pipelineutilities.add_files_to_json_object import AddFilesToJsonObject
from pipelineuntilties.add_paths_to_json_object import AddPathsToJsonObject


def run(event: dict, context: dict):
    """ Run the process to retrieve and process ArchivesSpace metadata.

    Information on the API can be found here:
        http://archivesspace.github.io/archivesspace/api/ """
    _supplement_event(event)
    _init_sentry()
    config = setup_pipeline_config(event)
    # config['rbsc-image-bucket'] = "libnd-smb-rbsc"
    start_time = time.time()
    time_to_break = datetime.now() + timedelta(seconds=config['seconds-to-allow-for-processing'])
    print("Will break after ", time_to_break)
    harvest_oai_eads_class = HarvestOaiEads(config)
    add_files_to_json_object_class = AddFilesToJsonObject(config)
    add_paths_to_json_object_class = AddPathsToJsonObject(config)
    ids = event.get("ids", [])
    while len(ids) > 0 and datetime.now() < time_to_break:
        nd_json = harvest_oai_eads_class.get_nd_json_from_archives_space_url(ids[0])
        if nd_json:
            nd_json = add_files_to_json_object_class.add_files(nd_json)
            nd_json = add_paths_to_json_object_class.add_paths(nd_json)
            write_s3_json(config['process-bucket'], os.path.join("json/", nd_json["id"] + '.json'), nd_json)
            print("ArchivesSpace ead_id = ", nd_json.get("id", ""), " source_system_url = ", ids[0], int(time.time() - start_time), 'seconds.')
            # in case we need to create CSVs
            # _export_json_as_csv(config, nd_json)
        del ids[0]
    event['archivesSpaceHarvestComplete'] = (len(ids) == 0)
    event['eadsSavedToS3'] = os.path.join(config['process-bucket'], config['process-bucket-csv-basepath'])
    return event


def _export_json_as_csv(config: dict, nd_json: dict):
    """ I'm leaving this here for now in case we need to create a CSV from the nd_json """
    convert_json_to_csv_class = ConvertJsonToCsv(config["csv-field-names"])
    csv_string = convert_json_to_csv_class.convert_json_to_csv(nd_json)
    s3_csv_file_name = os.path.join(config['process-bucket-csv-basepath'], nd_json["id"] + '.csv')
    write_s3_file(config['process-bucket'], s3_csv_file_name, csv_string)


def _supplement_event(event: dict) -> dict:
    if 'archivesSpaceHarvestComplete' not in event:
        event['archivesSpaceHarvestComplete'] = False
    if 'ssm_key_base' not in event and 'SSM_KEY_BASE' in os.environ:
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']
    if 'ids' not in event:  # seed with all archvesspace records we have been exporting prior to 5/12/2020
        event["ids"] = [
            "https://archivesspace.library.nd.edu/repositories/3/resources/1644",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1390",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1409",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1411",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1412",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1414",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1421",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1424",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1425",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1426",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1428",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1429",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1430",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1431",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1432",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1433",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1434",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1435",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1436",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1437",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1439",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1441",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1442",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1443",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1444",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1445",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1446",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1447",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1448",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1450",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1452",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1453",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1454",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1462",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1466",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1473",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1479",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1484",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1492",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1495",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1506",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1517",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1524",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1528",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1567",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1568",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1569",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1570",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1571",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1576",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1582",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1631",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1644",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1989",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1993",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1994",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1995",
            "https://archivesspace.library.nd.edu/repositories/3/resources/2000",
            "https://archivesspace.library.nd.edu/repositories/3/resources/2038"
        ]

    return event


def _init_sentry():
    if 'SENTRY_DSN' in os.environ:
        sentry_sdk.init(dsn=os.environ['SENTRY_DSN'], integrations=[AwsLambdaIntegration()])


# setup:
# export SSM_KEY_BASE=/all/new-csv
# aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
# python -c 'from handler import *; test()'
def test(identifier=""):
    """ test exection """
    filename = 'event.json'
    if os.path.exists(filename):
        with io.open(filename, 'r', encoding='utf-8') as json_file:
            event = json.load(json_file)
    else:
        event = {}
        event["local"] = False
        # event["ids"] = [
        #     "https://archivesspace.library.nd.edu/repositories/3/resources/1631",
        #     "https://archivesspace.library.nd.edu/repositories/3/resources/1644"
        # ]
        # event["ids"] = ["https://archivesspace.library.nd.edu/repositories/3/resources/1492"]  # Parsons Journals
        event["ids"] = ["https://archivesspace.library.nd.edu/repositories/3/resources/1524"]

    event = run(event, {})

    if not event['archivesSpaceHarvestComplete']:
        with open('event.json', 'w') as f:
            json.dump(event, f, indent=2)
    else:
        try:
            os.remove('event.json')
        except FileNotFoundError:
            pass
    print(event)
