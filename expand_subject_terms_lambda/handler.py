# handler.py
""" Module to launch application to expand subject terms """

import _set_path  # noqa
import io
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from pipelineutilities.pipeline_config import setup_pipeline_config  # noqa: E402
import sentry_sdk   # noqa: E402
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration  # noqa: E402
from dynamo_query_functions import get_subject_terms_needing_expanded
from expand_subject_terms import expand_subject_term

if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(dsn=os.environ['SENTRY_DSN'], integrations=[AwsLambdaIntegration()])


def run(event, _context):
    """ Run the process to retrieve and process Aleph metadata. """
    _supplement_event(event)
    config = setup_pipeline_config(event)
    if config:
        time_to_break = datetime.now() + timedelta(seconds=config['seconds-to-allow-for-processing'])
        print("Will break after ", time_to_break)
        subject_terms = get_subject_terms_needing_expanded(config.get('website-metadata-tablename'), 1)
        print('count of subject terms we need to expand = ', len(subject_terms))
        while len(subject_terms):
            subject = subject_terms.pop(0)
            expand_subject_term(subject, config.get('website-metadata-tablename'))
            if datetime.now() > time_to_break:
                break
        if len(subject_terms) == 0:
            event['expandSubjectTermsComplete'] = True
        if event["expandSubjectTermsExecutionCount"] >= event["maximumExpandSubjectTermsExecutions"]:
            event['expandSubjectTermsComplete'] = True

    return event


def _supplement_event(event):
    """ Add additional nodes to event if they do not exist. """
    if 'local' not in event:
        event['local'] = False
    if 'ssm_key_base' not in event and 'SSM_KEY_BASE' in os.environ:
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']
    if 'local-path' not in event:
        event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
    event['expandSubjectTermsComplete'] = event.get('expandSubjectTermsComplete', False)
    event['expandSubjectTermsExecutionCount'] = event.get('expandSubjectTermsExecutionCount', 0) + 1
    event['maximumExpandSubjectTermsExecutions'] = 10
    return


# setup:
# export SSM_KEY_BASE=/all/stacks/steve-manifest
# aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
# python -c 'from handler import *; test()'

def test():
    """ test exection """
    filename = 'event.json'
    if os.path.exists(filename):
        with io.open(filename, 'r', encoding='utf-8') as json_file:
            event = json.load(json_file)
    else:
        event = {}
        event['local'] = False
        event['seconds-to-allow-for-processing'] = 3550

    event = run(event, {})

    if not event['expandSubjectTermsComplete']:
        with open('event.json', 'w') as output_file:
            json.dump(event, output_file, indent=2)
    else:
        try:
            os.remove('event.json')
        except FileNotFoundError:
            pass

    print(event)
