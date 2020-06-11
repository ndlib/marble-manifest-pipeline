# curate_api.py
import time
import json
from datetime import datetime
import dependencies.requests
import os
from dependencies.sentry_sdk import capture_exception
from translate_curate_json_node import TranslateCurateJsonNode
from create_standard_json import CreateStandardJson
from convert_json_to_csv import ConvertJsonToCsv
from pipelineutilities.s3_helpers import write_s3_file, write_s3_json


class CurateApi():
    """ This performs all Curate API processing """
    def __init__(self, config, event, time_to_break):
        self.config = config
        self.event = event
        self.curate_header = {"X-Api-Token": self.config["curate-token"]}
        self.start_time = time.time()
        self.time_to_break = time_to_break
        self.translate_curate_json_node_class = TranslateCurateJsonNode(config)
        self.save_standard_json_locally = event.get("local", False)
        self.save_curate_json_locally = event.get("local", False)
        self.create_standard_json_class = CreateStandardJson(config)
        self.local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"

    def get_curate_items(self, ids: list) -> bool:
        """ Given a list of ids, process each one that corresponds to a Curate item """
        aborted_processing = False
        for id in list(ids):  # iterate over a copy of the list, so we can remove items from the original list
            if "und:" in id:
                curate_id = id.replace("und:", "")  # strip namespace
                self.get_curate_item(curate_id)
                ids.remove(id)
            if datetime.now() > self.time_to_break and len(ids) > 0:
                aborted_processing = True
                break
        return not(aborted_processing)

    def get_curate_item(self, id: str) -> dict:
        """ Get json metadata for a curate item given an item id
            Note: query is of the form: curate-server-base-url + "/api/items/<pid>" """
        standard_json = {}
        url = self.config["curate-server-base-url"] + "/api/items/" + id
        curate_json = self._get_json_given_url(url)
        members = []
        if "membersUrl" in curate_json:
            members = self._get_members_list(curate_json['membersUrl'], id, 100, False)
            members = self._get_members_details(members)
            curate_json["members"] = members
        if self.save_curate_json_locally:
            with open(self.local_folder + "test/" + id + "_curate.json", "w") as output_file:
                json.dump(curate_json, output_file, indent=2, ensure_ascii=False)
        standard_json = self.translate_curate_json_node_class.build_json_from_curate_json(curate_json, "root", {})
        if self.save_standard_json_locally:
            with open(self.local_folder + "test/" + id + "_preliminary_standard.json", "w") as output_file:
                json.dump(standard_json, output_file, indent=2, ensure_ascii=False)
        standard_json = self.create_standard_json_class.build_standard_json_tree(standard_json, members)
        if standard_json:
            if self.save_standard_json_locally:
                with open(self.local_folder + "test/" + id + "_nd.json", "w") as output_file:
                    json.dump(standard_json, output_file, indent=2, ensure_ascii=False)
            else:
                _save_json_to_s3(self.config['process-bucket'], os.path.join("json/", standard_json['id'] + '.json'), standard_json)
                _export_json_as_csv(self.config, standard_json)
        return standard_json

    def _get_members_list(self, url: str, parent_id: str, rows_to_return: int, testing_mode: bool = False) -> list:
        """ Call API to return members of a collection (or sub-collection)
            Note: query is of the form:  curate-server-base-url + "/api/items?part_of=<pid>" """
        results = []
        if rows_to_return > 100:
            rows_to_return = 100  # Curate API prohibits returning more than 100 rows, so let's try to retrieve the max possible
        url = url + "&rows=" + str(rows_to_return)
        i = 1
        while url and "part_of" in url:
            member_results = self._get_json_given_url(url)
            if "results" in member_results:
                for item in member_results["results"]:
                    id = item["id"]
                    if id != parent_id:
                        node = {}
                        node[id] = item
                        results.append(node)
                        i += 1
                        if testing_mode and i > 3:  # early out for testing
                            break
            url = self._get_next_page_url(member_results)
            if testing_mode:
                break
        return results

    def _get_members_details(self, members_json: dict, testing_mode: bool = False) -> dict:
        """ For each member, do an API call to get all metadata details we know about. """
        for member in members_json:
            for _key, value in member.items():
                # Intentionally skip datasets, since the API will not allow us to download those.
                if "itemUrl" in value and value.get("type", "") not in ["Dataset"]:
                    details_json = self._get_json_given_url(value["itemUrl"])
                    for details_key, details_value in details_json.items():
                        value[details_key] = details_value
                    if testing_mode:
                        break
            if testing_mode:
                break
        return members_json

    def _get_next_page_url(self, json_member_results: dict) -> str:
        """ For results with pagination, get the next url to be processed """
        url = None
        if "pagination" in json_member_results:
            url = json_member_results["pagination"].get("nextPage", None)
        return url

    def _get_json_given_url(self, url: str) -> dict:
        """ Return json from URL."""
        json_response = {}
        print("calling url =", url, int(time.time() - self.start_time), 'seconds.')
        try:
            json_response = dependencies.requests.get(url, headers=self.curate_header).json()
        except ConnectionRefusedError as e:
            print('Connection refused on url ' + url)
            capture_exception(e)
        except Exception as e:  # noqa E722 - intentionally ignore warning about bare except
            print('Error caught trying to process url ' + url)
            capture_exception(e)
        return json_response


def _export_json_as_csv(config: dict, nd_json: dict):
    """ I'm leaving this here for now until we no longer need to create a CSV from the nd_json """
    convert_json_to_csv_class = ConvertJsonToCsv(config["csv-field-names"])
    csv_string = convert_json_to_csv_class.convert_json_to_csv(nd_json)
    s3_csv_file_name = os.path.join(config['process-bucket-csv-basepath'], nd_json["id"] + '.csv')
    write_s3_file(config['process-bucket'], s3_csv_file_name, csv_string)


def _save_json_to_s3(s3_bucket_name: str, json_file_name: str, json_record: dict) -> bool:
    try:
        write_s3_json(s3_bucket_name, json_file_name, json_record)
        results = True
    except Exception:
        results = False
    return results
