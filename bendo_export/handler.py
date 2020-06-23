# handler.py
""" Module to launch application """

import _set_path  # noqa
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
import requests
from pipelineutilities.pipeline_config import setup_pipeline_config, load_config_ssm  # noqa: E402
import sentry_sdk   # noqa: E402
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration  # noqa: E402
from dependencies.sentry_sdk import capture_exception


if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(dsn=os.environ['SENTRY_DSN'], integrations=[AwsLambdaIntegration()])


def run(event: dict, context: dict) -> dict:
    """ Run the process to retrieve and process Aleph metadata. """
    _supplement_event(event)

    config = setup_pipeline_config(event)
    if config:
        time_to_break = datetime.now() + timedelta(seconds=config['seconds-to-allow-for-processing'])
        print("Will break after ", time_to_break)
        curate_config = load_config_ssm(config['curate_keys_ssm_base'])
        config.update(curate_config)

        unique_bendo_items = _get_unique_list_of_bendo_items(config)
        # print("unique_bendo_items = ", unique_bendo_items)
        unique_bendo_items = ['zp38w953h0s']  # , 'pv63fx74g23']  # overwrite for testing
        _get_detail_for_all_items(config, unique_bendo_items)
    return event


def _get_detail_for_all_items(config: dict, unique_bendo_items: list) -> dict:
    stop_after = 5
    i = 0
    for each_item in unique_bendo_items:
        i += 1
        item_metadata = _get_metadata_for_single_item(each_item, config)
        print("item_metadata = ", item_metadata)
        if i > stop_after:
            break
    return item_metadata


def _get_unique_list_of_bendo_items(config: dict) -> dict:
    unique_bendo_items = []
    url = config["bendo-server-base-url"] + "/bundle/list/"
    json_list = _get_json_given_url(url, config)
    for each_item in json_list:
        item_string = re.sub(r"-.*$", "", each_item)
        if item_string not in unique_bendo_items:
            unique_bendo_items.append(item_string)
    unique_bendo_items.sort()
    return unique_bendo_items


def _get_metadata_for_single_item(item_id: str, config: dict) -> dict:
    url = config["bendo-server-base-url"] + "/item/" + item_id + "?format=json"
    item_metadata = _get_json_given_url(url, config)
    files_info = _get_files_for_bendo_item(item_metadata, config)
    return files_info


def _get_files_for_bendo_item(item_metadata: dict, config: dict) -> dict:
    files_dict = _get_newest_version_of_each_file_from_versions(item_metadata, config)
    print("files_dict = ", files_dict)
    for file in files_dict:
        # print("file = ", file)
        blob_metadata = _get_blob_metadata_given_blob_id(item_metadata, files_dict[file]["blobId"])
        files_dict[file].update(blob_metadata)
    # print("updated files_dict = ", files_dict)
    return files_dict


def _get_blob_metadata_given_blob_id(item_metadata: dict, blob_id: int) -> dict:
    for blob in item_metadata["Blobs"]:
        if blob["ID"] == blob_id:
            return blob
    return {}


def _get_newest_version_of_each_file_from_versions(item_metadata: dict, config: dict) -> dict:
    results = {}
    i = len(item_metadata["Versions"])
    while i > 0:
        i -= 1
        version_no = item_metadata["Versions"][i]["ID"]
        # print('item_metadata["Versions"][i]["Slots"] = ', item_metadata["Versions"][i]["Slots"])
        if len(item_metadata["Versions"][i]["Slots"]) == 1:
            for key, value in item_metadata["Versions"][i]["Slots"].items():
                if key not in results:
                    new_node = {}
                    new_node["version"] = version_no
                    new_node["blobId"] = value
                    results[key] = new_node
        else:
            for slot in item_metadata["Versions"][i]["Slots"]:
                print("slot = ", slot)
                for key, value in slot.items():
                    if key not in results:
                        new_node = {}
                        new_node["version"] = version_no
                        new_node["blobId"] = value
                        results[key] = new_node
    print("results = ", results)
    return results


def _get_json_given_url(url: str, config: dict) -> dict:
    """ Return json from URL."""
    json_response = {}
    try:
        json_response = requests.get(url, auth=(config["bendo-token"], "")).json()
    except ConnectionRefusedError as e:
        print('Connection refused on url ' + url)
        capture_exception(e)
    except Exception as e:  # noqa E722 - intentionally ignore warning about bare except
        print('Error caught trying to process url ' + url)
        capture_exception(e)
    return json_response


def _supplement_event(event: dict) -> dict:
    """ Add additional nodes to event if they do not exist. """
    if 'bendoHarvestComplete' not in event:
        event['bendoHarvestComplete'] = False
    if 'local' not in event:
        event['local'] = False
    if 'ssm_key_base' not in event and 'SSM_KEY_BASE' in os.environ:
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']
    if 'local-path' not in event:
        event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
    return


# setup:
# export SSM_KEY_BASE=/all/new-csv
# aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
# python -c 'from handler import *; test()'

# testing:
# python 'run_all_tests.py'
def test(identifier=""):
    """ test exection """
    event = {}
    event = run(event, {})
    print(event)
