# handler.py
""" Module to launch application """

import _set_path  # noqa
import io
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from harvest_aleph_marc import HarvestAlephMarc  # noqa: #402
from pipelineutilities.pipeline_config import setup_pipeline_config  # noqa: E402
import sentry_sdk   # noqa: E402
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration  # noqa: E402
from dynamo_save_functions import save_source_system_record, save_website_record

if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(dsn=os.environ['SENTRY_DSN'], integrations=[AwsLambdaIntegration()])


def run(event, _context):
    """ Run the process to retrieve and process Aleph metadata. """
    _supplement_event(event)
    config = setup_pipeline_config(event)
    if config:
        marc_records_url = "https://alephprod.library.nd.edu/aleph_tmp/marble.mrc"
        # marc_records_url = "https://alephprep.library.nd.edu/aleph_tmp/marble.mrc"
        time_to_break = datetime.now() + timedelta(seconds=config['seconds-to-allow-for-processing'])
        print("Will break after ", time_to_break)
        if event.get('alephExecutionCount', 0) == 1 and not event.get('local'):
            save_source_system_record(config.get('website-metadata-tablename'), 'Aleph')
            # I know these next 2 lines do not belong here.  I'm temporarily putting them here just to make sure Dynamo is initialized.  We will eventually find a better place to put this.
            save_website_record(config.get('website-metadata-tablename'), 'Marble')
            save_website_record(config.get('website-metadata-tablename'), 'Inquisitions')
        harvest_marc_class = HarvestAlephMarc(config, event, marc_records_url, time_to_break)
        records_harvested = harvest_marc_class.process_marc_records_from_stream()
        key = 'countHarvestedLoop' + str(event["alephExecutionCount"])
        event[key] = records_harvested
        if event["alephExecutionCount"] >= event["maximumAlephExecutions"]:
            event['alephHarvestComplete'] = True

    return event


def _supplement_event(event):
    """ Add additional nodes to event if they do not exist. """
    if 'ids' not in event:
        event['ids'] = []
    if 'local' not in event:
        event['local'] = False
    if 'ssm_key_base' not in event and 'SSM_KEY_BASE' in os.environ:
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']
    if 'local-path' not in event:
        event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
    event['alephHarvestComplete'] = event.get('alephHarvestComplete', False)
    event['alephExecutionCount'] = event.get('alephExecutionCount', 0) + 1
    event['maximumAlephExecutions'] = 10
    event['maxAlephIdProcessed'] = event.get('maxAlephIdProcessed', '')
    return


# setup:
# export SSM_KEY_BASE=/all/stacks/steve-manifest
# aws-vault exec testlibnd-superAdmin
# python -c 'from handler import *; test()'

# testing:
# python 'run_all_tests.py'
def test():
    """ test exection """
    filename = 'event.json'
    if os.path.exists(filename):
        with io.open(filename, 'r', encoding='utf-8') as json_file:
            event = json.load(json_file)
    else:
        event = {}
        event['local'] = False
        event['seconds-to-allow-for-processing'] = 9000
        # event['ids'] = ['002468275']
        # event['ids'] = ['001586302', '001587052', '001587050', '001588845', '001590687']
        # event['forceSaveStandardJson'] = True
    event = run(event, {})

    if not event['alephHarvestComplete']:
        with open('event.json', 'w') as output_file:
            json.dump(event, output_file, indent=2)
    else:
        try:
            os.remove('event.json')
        except FileNotFoundError:
            pass

    print(event)
