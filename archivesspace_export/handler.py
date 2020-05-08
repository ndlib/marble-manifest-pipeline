# handler.py
""" Module to launch application """
import _set_path  # noqa
import json
import io
import os
from pathlib import Path
from harvest_oai_eads import HarvestOaiEads  # noqa: #502
from file_system_utilities import delete_file  # noqa: E402
from pipelineutilities.pipeline_config import setup_pipeline_config  # noqa: E402
import sentry_sdk as sentry_sdk  # noqa: E402
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration  # noqa: E402
from pipelineutilities.s3_helpers import write_s3_json, write_s3_file
from convert_json_to_csv import ConvertJsonToCsv
from pipelineutilities.search_files import id_from_url, crawl_available_files  # noqa: #402
from add_files_to_nd_json import AddFilesToNdJson

def run(event, context):
    """ Run the process to retrieve and process ArchivesSpace metadata.

    Information on the API can be found here:
        http://archivesspace.github.io/archivesspace/api/ """
    _supplement_event(event)
    _init_sentry()
    config = setup_pipeline_config(event)
    config['rbsc-image-bucket'] = "libnd-smb-rbsc"
    if config:
        harvest_oai_eads_class = HarvestOaiEads(config, event)
        # convert_json_to_csv_class = ConvertJsonToCsv(config["csv-field-names"])
        ids = event.get("ids", [])
        while len(ids) > 0:
            # hash_of_available_files = crawl_available_files(config)
            # with open('./test/hash_of_available_files.json', 'w') as f:
            #     json.dump(hash_of_available_files, f, indent=2, default=str)

            nd_json = harvest_oai_eads_class.get_nd_json_from_archives_space_url(ids[0])
            if nd_json:
                with open(nd_json["id"] + '.json', 'w') as f:
                    json.dump(nd_json, f, indent=2)
                add_files_to_nd_json_class = AddFilesToNdJson(config)
                nd_json_with_files = add_files_to_nd_json_class.add_files(nd_json)
                with open(nd_json_with_files["id"] + '_with_files.json', 'w') as f:
                    json.dump(nd_json_with_files, f, indent=2)
                # write_s3_json(config['process-bucket'], os.path.join("json/", nd_json["id"] + '.json'), nd_json)
                # event['eadsSavedToS3'] = os.path.join(config['process-bucket'], config['process-bucket-csv-basepath'])
                # csv_string = convert_json_to_csv_class.convert_json_to_csv(nd_json)
                # s3_csv_file_name = os.path.join(config['process-bucket-csv-basepath'], nd_json["id"] + '.csv')
                # write_s3_file(config['process-bucket'], s3_csv_file_name, csv_string)
                # subsequent steps:
                # prune if no files
                # add files
            del ids[0]
        event['eadHarvestComplete'] = (len(ids) == 0)
    return event


def _supplement_event(event):
    if 'eadHarvestComplete' not in event:
        event['eadHarvestComplete'] = False
    if 'local' not in event:
        event['local'] = False
    if 'ssm_key_base' not in event and 'SSM_KEY_BASE' in os.environ:
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']
    if 'local-path' not in event:
        event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
    return


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
            "https://archivesspace.library.nd.edu/repositories/3/resources/2008",
            "https://archivesspace.library.nd.edu/repositories/3/resources/2038"
        ]
        event["ids"] = [
            "https://archivesspace.library.nd.edu/repositories/3/resources/1631",
            "https://archivesspace.library.nd.edu/repositories/3/resources/1644"
        ]
        event["ids"] = ["https://archivesspace.library.nd.edu/repositories/3/resources/1492"]  # Parsons Journals

    event = run(event, {})

    if not event['eadHarvestComplete']:
        with open('event.json', 'w') as f:
            json.dump(event, f, indent=2)
    else:
        delete_file('.', 'event.json')
    print(event)
