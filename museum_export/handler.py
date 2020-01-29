# handler.py
""" Module to launch application """

import os
import sys
import json
from pathlib import Path
where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am)
sys.path.append(where_i_am + "/dependencies")
from dependencies.pipelineutilities.pipeline_config import get_pipeline_config, load_config_ssm  # noqa: E402
import dependencies.sentry_sdk as sentry_sdk  # noqa: E402
from dependencies.sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration  # noqa: E402


# import sentry_sdk  # noqa: E402
# from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration  # noqa: E402
# from get_config import get_config  # noqa: E402
from process_web_kiosk_json_metadata import processWebKioskJsonMetadata  # noqa: E402
from dependencies.pipelineutilities.google_utilities import establish_connection_with_google_api, execute_google_query, build_google_query_string  # noqa: #402

# from file_system_utilities import delete_file  # noqa: E402


# config = get_config()
# config['process-bucket'] = 'new-csv-processbucket-10dr776tnq9be'


def run(event, context):
    """ run the process to retrieve and process web kiosk metadata """
    _suplement_event(event)
    config = get_pipeline_config(event)
    config = load_config_ssm(config['google_keys_ssm_base'], config)
    config = load_config_ssm(config['museum_keys_ssm_base'], config)
    # print("config=", config)
    # print("config-process-bucket=", config['process-bucket'])
    # config['process-bucket'] = 'new-csv-processbucket-10dr776tnq9be'

    if config != {}:
        google_credentials = json.loads(config["museum-google-credentials"])
        google_connection = establish_connection_with_google_api(google_credentials)

        if 'SENTRY_DSN' in os.environ:
            sentry_sdk.init(dsn=os.environ['SENTRY_DSN'], integrations=[AwsLambdaIntegration()])
        if 'MODE' in os.environ:
            mode = os.environ['MODE']
        else:
            mode = 'full'
        jsonWebKioskClass = processWebKioskJsonMetadata(config, google_connection)
        composite_json = jsonWebKioskClass.get_composite_json_metadata(mode)
        if composite_json != {}:
            jsonWebKioskClass.process_composite_json_metadata()
        else:
            print('No JSON to process')
    else:
        print('No configuration defined.  Unable to continue.')
    return event


def _suplement_event(event):
    # if 'ids' not in event:
    #     event['ids'] = []
    # if 'local' not in event:
    #     event['local'] = True
    if 'ssm_key_base' not in event and 'SSM_KEY_BASE' in os.environ:
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']
    if 'local-path' not in event:
        event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
    return


# setup:
# cd marble-web-kiosk-export
# aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
# # export SSM_KEY_BASE=/all/marble-data-processing/test
# export SSM_KEY_BASE=/all/manifest-pipeline-v3
# export MODE=full
# python -c 'from handler import *; test()'
# python 'run_all_tests.py'
def test():
    """ test exection """
    event = {}

    print(run(event, {}))
