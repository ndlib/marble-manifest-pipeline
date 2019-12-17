# handler.py
""" Module to launch application """

import json
from pathlib import Path
import os
import sys
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am)
sys.path.append(where_i_am + "/dependencies")
from get_config import get_config  # noqa: E402
from google_utilities import establish_connection_with_google_api  # noqa: E402
from send_object_to_manifest_pipeline import process_objects  # noqa: E402

config = get_config()
sentry_sdk.init(
    dsn=os.environ['SENTRY_DSN'],
    integrations=[AwsLambdaIntegration()]
)


def run(event, context):
    """ run the process to retrieve and process web kiosk metadata """
    objects_needing_processed = []
    if config != {}:
        google_credentials = config['google']['credentials']
        google_connection = establish_connection_with_google_api(google_credentials)
        if 'objectsNeedingProcessed' in event:
            objects_needing_processed = event['objectsNeedingProcessed']
            objects_needing_processed = process_objects(google_connection, config, objects_needing_processed)
    else:
        print('No configuration defined.  Unable to continue.')
    event['objectsNeedingProcessed'] = objects_needing_processed
    if len(objects_needing_processed) == 0:
        event['populatePipelineCompleted'] = True
    return event


# setup:
# export SSM_MARBLE_DATA_PROCESSING_KEY_BASE=/all/marble-data-processing/test
# export SSM_KEY_BASE=/all/manifest-pipeline-v3
#
# aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
#
# python -c 'from handler import *; test()'
def test():
    """ test execution """
    event = {}
    current_path = str(Path(__file__).parent.absolute())
    file_name = current_path + '/../example/recently_changed_objects_needing_processed/event_after_find_objects.json'  # noqa: E501
    # if os.path.isfile(file_name):
    print(file_name)
    with open(file_name, encoding='utf-8') as data_file:
        event = json.loads(data_file.read())
        data_file.close()
    print(event)
    event = run(event, {})
    objects_needing_processed = event['objectsNeedingProcessed']

    if len(objects_needing_processed) == 0:
        try:
            os.remove(file_name)
        except FileNotFoundError:
            pass
    else:
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(objects_needing_processed, f, ensure_ascii=False, indent=4, sort_keys=True)
            f.close()
