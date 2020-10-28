"""This module saves a listing of contents of standard json files by source.
    These results are saved into the manifest-server-bucket.
    These are served by calls to cloudfront here:  https://presentation-iiif.library.nd.edu/collections/all
    These are used by redbox."""

import _set_path  # noqa
import os
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
from pipeline_config import setup_pipeline_config
from collections_api import CollectionsApi


if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(
        dsn=os.environ['SENTRY_DSN'],
        integrations=[AwsLambdaIntegration()]
    )


def run(event, _context):
    """ Main run routine for module """
    """ Note:  on 10/28/2020 we made a change to step functions that run these lambdas to run step funcitons in parallel.
        As a result of this change, this module may accept as "event" either a dictionary, or an array of dictionaries. """
    myevent = _get_appropriate_event_dict(event)
    myevent['local'] = myevent.get("local", False)
    if 'ssm_key_base' not in myevent and 'SSM_KEY_BASE' in os.environ:
        myevent['ssm_key_base'] = os.environ['SSM_KEY_BASE']
    config = setup_pipeline_config(myevent)
    collections_api_class = CollectionsApi(config)
    collections_api_class.save_collection_details(['aleph', 'archivesspace', 'curate', 'embark'])
    myevent['collectionsApiComplete'] = True
    event = _update_original_event(event, myevent)
    return event


def _get_appropriate_event_dict(event: (dict, list)) -> dict:
    """ If dict is passed, return dict. If list is passed, find dict for "collections".
        If none exist, create one, and append to list.  Return dict for collections. """
    if isinstance(event, dict):
        return event
    elif isinstance(event, list):
        i = _find_right_dict_in_list(event, 'collectionsApiComplete')
        if i:
            return event[i]
        node = {'collectionsApiComplete': False, 'local': False}
        event.append(node)
        return node


def _find_right_dict_in_list(event: list, key_to_find: str) -> int:
    """ find right dict in list based on key_to_find """
    for i, each_dict in enumerate(event):
        if key_to_find in each_dict:
            return i
    return None


def _update_original_event(event: list, myevent: dict) -> dict:
    if isinstance(event, dict):
        event = myevent
    elif isinstance(event, list):
        i = _find_right_dict_in_list(event, 'collectionsApiComplete')
        if i:
            for k, v in myevent.items():
                event[i][k] = v
    return event


# export SSM_KEY_BASE=/all/stacks/steve-manifest
# aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
# python -c 'from handler import *; test()'
def test():
    """ test """
    event = {}
    event['local'] = False
    if 'ssm_key_base' not in event and 'SSM_KEY_BASE' in os.environ:
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']
    else:
        event['ssm_key_base'] = '/all/new-csv'

    event = [
        {
            "Comment": "Insert your JSON here",
            "ids": [],
            "local": False,
            "ssm_key_base": "/all/stacks/steve-manifest",
            "local-path": "/var/task/../example/",
            "alephHarvestComplete": True
        },
        {
            "Comment": "Insert your JSON here",
            "archivesSpaceHarvestComplete": True,
            "ssm_key_base": "/all/stacks/steve-manifest",
            "ids": [],
            "eadsSavedToS3": "steve-manifest-processbuckete5460fc2-ulh25u5q0hlq/json"
        },
        {
            "Comment": "Insert your JSON here",
            "curateHarvestComplete": True,
            "local": False,
            "ssm_key_base": "/all/stacks/steve-manifest",
            "local-path": "/var/task/../example/",
            "curate_execution_count": 1,
            "max_curate_executions": 5,
            "ids": []
        },
        {
            "Comment": "Insert your JSON here",
            "ssm_key_base": "/all/stacks/steve-manifest",
            "local-path": "/var/task/../example/",
            "museumHarvestComplete": True,
            "museum_execution_count": 6,
            "maximum_museum_executions": 15
        },
        {
            "Comment": "Insert your JSON here",
            "local-path": "/var/task/../example/",
            "objectFilesApiComplete": True,
            "objectFilesApi_execution_count": 1,
            "maximum_objectFilesApi_executions": 10,
            "ssm_key_base": "/all/stacks/steve-manifest",
            "local": False,
            "objectFilesApiDirectoriesCount": 4793
        }
    ]

    print(run(event, {}))
