# handler.py
""" Module to launch application """

import json
from pathlib import Path
import os
import sys
where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am)
sys.path.append(where_i_am + "/dependencies")
from get_config import get_config  # noqa: E402
from google_utilities import establish_connection_with_google_api  # noqa: E402
from find_objects import find_objects_needing_processed  # noqa: E402

config = get_config()


def run(event, context):
    """ run the process to retrieve and process metadata files """
    if config != {}:
        google_credentials = config['google']['credentials']
        google_connection = establish_connection_with_google_api(google_credentials)
        repositories = ['museum']  # ['museum', 'library']
        for repository in repositories:
            objects_needing_processed = find_objects_needing_processed(google_connection, config, repository)
    else:
        print('No configuration defined.  Unable to continue.')
    return objects_needing_processed


# setup:
# export SSM_MARBLE_DATA_PROCESSING_KEY_BASE=/all/marble-data-processing/test
# export SSM_KEY_BASE=/all/manifest-pipeline-v3
#
# aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
#
# python -c 'from handler import *; test()'
def test():
    """ test execution """
    data = {}
    objects_needing_processed = run(data, {})
    current_path = str(Path(__file__).parent.absolute())
    file_name = current_path + '/../example/recently_changed_objects_needing_processed/objects_needing_processed.json'
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(objects_needing_processed, f, ensure_ascii=False, indent=4, sort_keys=True)
