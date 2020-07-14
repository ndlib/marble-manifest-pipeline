# curate_api.py
import time
import json
from datetime import datetime
import dependencies.requests
import os
import io
from dependencies.sentry_sdk import capture_exception
from translate_curate_json_node import TranslateCurateJsonNode
from create_standard_json import CreateStandardJson
from pipelineutilities.save_standard_json import save_standard_json


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

    def get_curate_item(self, id: str) -> dict:  # noqa: C901  - Added code to accommodate export architectural lantern slides looping and restarting
        """ Get json metadata for a curate item given an item id
            Note: query is of the form: curate-server-base-url + "/api/items/<pid>" """
        standard_json = {}
        url = self.config["curate-server-base-url"] + "/api/items/" + id
        filename = self.local_folder + "test/" + id + "_curate.json"
        file_exists_flag = os.path.exists(filename)
        if self.attempting_huge_export_with_resumption_flag and file_exists_flag:
            with io.open(filename, 'r', encoding='utf-8') as json_file:
                curate_json = json.load(json_file)
        else:
            curate_json = self._get_json_given_url(url)
            if self.save_standard_json_locally:
                with open(self.local_folder + "test/" + id + "_curate.json", "w") as output_file:
                    json.dump(curate_json, output_file, indent=2, ensure_ascii=False)
        members = []
        if "membersUrl" in curate_json:
            if not curate_json.get("members", False):
                print("getting members list")
                members = self._get_members_list(curate_json['membersUrl'], id, 100, False)
                curate_json["members"] = members
                # if self.save_standard_json_locally:
                #     with open(self.local_folder + "test/" + id + "_curate_with_members_list.json", "w") as output_file:
                #         json.dump(curate_json, output_file, indent=2, ensure_ascii=False)
            else:
                members = curate_json["members"]
            filename = self.local_folder + "test/" + id + "_members_json.json"
            file_exists_flag = os.path.exists(filename)
            if file_exists_flag:
                with io.open(filename, 'r', encoding='utf-8') as json_file:
                    members = json.load(json_file)
            while self._more_unprocessed_members_exist(members):
                members = self._get_members_details(members, False, id, 20)
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
                with open(self.local_folder + "test/" + id + "_standard.json", "w") as output_file:
                    json.dump(standard_json, output_file, indent=2, ensure_ascii=False)
            else:
                save_standard_json(self.config, standard_json)
        return standard_json

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
                    id = item["id"]
                    if id != parent_id:
                        node = {}
                        node[id] = item
                        results.append(node)
                        i += 1
                        if testing_mode:  # early out for testing
                            break
            url = self._get_next_page_url(member_results)
            if testing_mode:
                break
        return results

    def _get_members_details(self, members_json: dict, testing_mode: bool = False, id: str = "", limit_record_count: int = 99999) -> dict:
        """ For each member, do an API call to get all metadata details we know about. """
        i = 0
        for member in members_json:
            if not member.get("detailsRetrieved", False):
                for _key, value in member.items():
                    # Intentionally skip datasets, since the API will not allow us to download those.
                    if "itemUrl" in value and value.get("type", "") not in ["Dataset"]:
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
                    print("i, limit = ", i, limit_record_count)
                    if self.save_standard_json_locally and self.attempting_huge_export_with_resumption_flag:
                        with open(self.local_folder + "test/" + id + "_members_json.json", "w") as output_file:
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
