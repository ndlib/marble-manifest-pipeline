"""
Curate_API
Calls the Curate API, massages results to create standard json output, which is then saved.
"""

import time
import json
from datetime import datetime
import os
import io
import dependencies.requests
from dependencies.sentry_sdk import capture_exception
from translate_curate_json_node import TranslateCurateJsonNode
from create_standard_json import CreateStandardJson
from pipelineutilities.standard_json_helpers import StandardJsonHelpers
from pipelineutilities.save_standard_json_to_dynamo import SaveStandardJsonToDynamo
from pipelineutilities.save_standard_json import save_standard_json
from save_json_to_dynamo import SaveJsonToDynamo
from dynamo_helpers import add_file_keys


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
        self.attempting_huge_export_with_resumption_flag = False
        self.save_json_to_dynamo_class = SaveJsonToDynamo(config, self.config.get('website-metadata-tablename', ''))

    def get_curate_items(self, ids: list) -> bool:
        """ Given a list of ids, process each one that corresponds to a Curate item """
        aborted_processing = False
        for item_id in list(ids):  # iterate over a copy of the list, so we can remove items from the original list
            if "und:" in item_id:
                curate_id = item_id.replace("und:", "")  # strip namespace
                self.get_curate_item(curate_id)
                ids.remove(item_id)
            if datetime.now() > self.time_to_break and len(ids) > 0:
                aborted_processing = True
                break
        return not aborted_processing

    def get_curate_item(self, item_id: str) -> dict:
        """ Get json metadata for a curate item given an item_id
            Note: query is of the form: curate-server-base-url + "/api/items/<pid>" """
        standard_json = {}
        curate_json = self._get_curate_json(item_id)
        members = []
        if "membersUrl" in curate_json:
            members = self._get_members_json(curate_json, item_id)
            while self._more_unprocessed_members_exist(members):
                members = self._get_members_details(members, False, item_id, 20)
                curate_json["members"] = members
                if self.save_curate_json_locally or self.attempting_huge_export_with_resumption_flag:
                    with open(self.local_folder + "test/" + item_id + "_curate.json", "w") as output_file:
                        json.dump(curate_json, output_file, indent=2, ensure_ascii=False)

        standard_json = self.translate_curate_json_node_class.build_json_from_curate_json(curate_json, "root", {})
        if self.save_standard_json_locally:
            with open(self.local_folder + "test/" + item_id + "_preliminary_standard.json", "w") as output_file:
                json.dump(standard_json, output_file, indent=2, ensure_ascii=False)
        standard_json = self.create_standard_json_class.build_standard_json_tree(standard_json, members)
        standard_json_helpers_class = StandardJsonHelpers(self.config)
        standard_json = standard_json_helpers_class.enhance_standard_json(standard_json)
        if standard_json:
            if self.save_standard_json_locally:
                with open(self.local_folder + "test/" + item_id + "_standard.json", "w") as output_file:
                    json.dump(standard_json, output_file, indent=2, ensure_ascii=False)
            else:
                export_all_files_flag = self.event.get('export_all_files_flag', False)
                save_standard_json(self.config, standard_json)
                save_standard_json_to_dynamo_class = SaveStandardJsonToDynamo(self.config)
                save_standard_json_to_dynamo_class.save_standard_json(standard_json, export_all_files_flag)
                self._save_curate_image_data_to_dynamo(standard_json)

        return standard_json

    def _get_curate_json(self, item_id: str) -> dict:
        """ If self.attempting_huge_export_with_resumption_flag, read file locally
            (if it exists), otherwise, get json using url to Curate API """
        curate_json = {}
        url = self.config["curate-server-base-url"] + "/api/items/" + item_id
        filename = self.local_folder + "test/" + item_id + "_curate.json"
        if self.attempting_huge_export_with_resumption_flag and os.path.exists(filename):
            with io.open(filename, 'r', encoding='utf-8') as json_file:
                curate_json = json.load(json_file)
        else:
            curate_json = self._get_json_given_url(url)
            if self.save_standard_json_locally or self.attempting_huge_export_with_resumption_flag:
                with open(self.local_folder + "test/" + item_id + "_curate.json", "w") as output_file:
                    json.dump(curate_json, output_file, indent=2, ensure_ascii=False)
        return curate_json

    def _get_members_json(self, curate_json: dict, item_id: str) -> dict:
        """ If self.attempting_huge_export_with_resumption_flag, read file locally
            (if it exists), otherwise, get json using url to Curate API """
        if not curate_json.get("members", False):
            print("getting members list")
            members = self._get_members_list(curate_json['membersUrl'], item_id, 100, False)
            curate_json["members"] = members
            if self.attempting_huge_export_with_resumption_flag:
                with open(self.local_folder + "test/" + item_id + "_curate.json", "w") as output_file:
                    json.dump(curate_json, output_file, indent=2, ensure_ascii=False)
        else:
            members = curate_json["members"]
        filename = self.local_folder + "test/" + item_id + "_members_json.json"
        if os.path.exists(filename):
            with io.open(filename, 'r', encoding='utf-8') as json_file:
                members = json.load(json_file)
        return members

    def _more_unprocessed_members_exist(self, members: dict) -> bool:
        for member in members:
            if not member.get("detailsRetrieved", False):
                return True
        return False

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
            # with open(self.local_folder + "test/member_results_get_json_given_url_parent_" + parent_id + ".json", "w") as output_file:
            #     json.dump(member_results, output_file, indent=2, ensure_ascii=False)
            if "results" in member_results:
                for item in member_results["results"]:
                    item_id = item["id"]
                    if item_id != parent_id:
                        node = {}
                        node[item_id] = item
                        results.append(node)
                        i += 1
                        if testing_mode:  # early out for testing
                            break
            url = self._get_next_page_url(member_results)
            if testing_mode:
                break
        return results

    def _get_members_details(self, members_json: dict, testing_mode: bool = False, item_id: str = "", limit_record_count: int = 99999) -> dict:
        """ For each member, do an API call to get all metadata details we know about. """
        i = 0
        for member in members_json:
            if not member.get("detailsRetrieved", False):
                for _key, value in member.items():
                    # Intentionally skip datasets, since the API will not allow us to download those.
                    if "itemUrl" in value and value.get("type", "") not in ["Audio", "Dataset"]:
                        details_json = self._get_json_given_url(value["itemUrl"])
                        # with open(self.local_folder + "test/" + _key + "_get_members_details_get_json_given_url.json", "w") as output_file:
                        #     json.dump(details_json, output_file, indent=2, ensure_ascii=False)
                        for details_key, details_value in details_json.items():
                            value[details_key] = details_value
                        if testing_mode:
                            break
                if testing_mode:
                    break
                i += 1
                member["detailsRetrieved"] = True
                if i > limit_record_count:
                    # print("i, limit = ", i, limit_record_count)
                    if self.save_standard_json_locally and self.attempting_huge_export_with_resumption_flag:
                        with open(self.local_folder + "test/" + item_id + "_members_json.json", "w") as output_file:
                            json.dump(members_json, output_file, indent=2, ensure_ascii=False)
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

    def _save_curate_image_data_to_dynamo(self, standard_json: dict):
        """ Save Curate image data to dynamo recursively """
        if standard_json.get('level', '') == 'file':
            new_dict = {i: standard_json[i] for i in standard_json if i != 'items'}
            new_dict['objectFileGroupId'] = new_dict['parentId']
            new_dict = add_file_keys(new_dict)
            self.save_json_to_dynamo_class.save_json_to_dynamo(new_dict)
        for item in standard_json.get('items', []):
            self._save_curate_image_data_to_dynamo(item)
