# handler.py
""" Module to launch application """
import _set_path  # noqa
import json
import io
import os
from pathlib import Path
from harvest_oai_eads import HarvestOaiEads  # noqa: #502
from file_system_utilities import delete_file  # noqa: E402
from pipelineutilities.pipeline_config import get_pipeline_config  # noqa: E402
import sentry_sdk as sentry_sdk  # noqa: E402
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration  # noqa: E402


def run(event, context):
    """ Run the process to retrieve and process ArchivesSpace metadata.

    Information on the API can be found here:
        http://archivesspace.github.io/archivesspace/api/ """
    _supplement_event(event)
    _init_sentry()
    config = get_pipeline_config(event)
    if config:
        resumption_token = event['resumptionToken']
        harvest_oai_eads_class = HarvestOaiEads(config, event)
        mode = _check_environment_variable('MODE', 'full')
        if not event['eadHarvestComplete']:
            resumption_token = harvest_oai_eads_class.harvest_relevant_eads(mode, resumption_token)
        event['resumptionToken'] = resumption_token
        event['eadHarvestComplete'] = (resumption_token is None)
        if not event['local']:
            event['eadsSavedToS3'] = os.path.join(config['process-bucket'], config['process-bucket-csv-basepath'])

    return event


def _supplement_event(event):
    if 'eadHarvestComplete' not in event:
        event['eadHarvestComplete'] = False
    if 'resumptionToken' not in event:
        event['resumptionToken'] = ""
    if 'ids' not in event:
        event['ids'] = []
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


def _check_environment_variable(variable_name, default):
    return_value = default
    if variable_name in os.environ:
        return_value = os.environ[variable_name]
    return return_value


# setup:
# export SSM_KEY_BASE=/all/new-csv
# export MODE=known
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
        mode = _check_environment_variable('MODE', 'full')
        if mode == 'identifiers':
            event['ids'] = ['oai:und//repositories/3/resources/1644']
        elif mode == 'ids':
            event['ids'] = ['BPP1001_EAD', 'MSNEA8011_EAD']

    event = run(event, {})

    if not event['eadHarvestComplete']:
        with open('event.json', 'w') as f:
            json.dump(event, f, indent=2)
    else:
        delete_file('.', 'event.json')
    print(event)
