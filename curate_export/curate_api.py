# curate_api.py
import time
import json
from datetime import datetime
import dependencies.requests
from dependencies.sentry_sdk import capture_exception
from translate_curate_json import TranslateCurateJson
from csv_from_json import CsvFromJson
from save_csv import SaveCsv


class CurateApi():
    """ This performs all Curate API processing """
    def __init__(self, config, event, time_to_break):
        self.config = config
        self.event = event
        self.curate_header = {"X-Api-Token": self.config["curate-token"]}
        self.start_time = time.time()
        self.time_to_break = time_to_break
        self.translate_curate_json_class = TranslateCurateJson(config, event)
        self.sequences_within_parent = {}
        self.save_standard_json_locally = False
        self.save_curate_json_locally = False
        self.save_standard_json_locally = event.get("local", False)
        self.save_curate_json_locally = event.get("local", False)
        self.csv_from_json_class = CsvFromJson(self.config.get("csv-field-names", []))
        self.save_csv_class = SaveCsv(config, event)

    def get_curate_items(self, ids):
        """ Given a list of ids, process each one """
        aborted_processing = False
        for id in list(ids):  # iterate over a copy of the list, so we can remove items from the original list
            if "und:" in id:
                curate_id = id.replace("und:", "")
                standard_json = self.get_curate_item(curate_id)
                csv_string = self.csv_from_json_class.return_csv_from_json(standard_json)
                self.save_csv_class.save_csv_file(curate_id, csv_string)
                ids.remove(id)
            if datetime.now() > self.time_to_break and len(ids) > 0:
                aborted_processing = True
                break
        return not(aborted_processing)

    def get_curate_item(self, id):
        """ Get json metadata for a curate item given an item id
            Note: query is of the form: curate-server-base-url + "/api/items/<pid>" """
        id = id.replace("und:", "")  # strip namespace if supplied
        url = self.config["curate-server-base-url"] + "/api/items/" + id
        curate_json = self._get_json_given_url(url)
        members = []
        if "membersUrl" in curate_json:
            members = self.get_members_list(curate_json['membersUrl'], id)
            members = self._get_members_details(members)
            curate_json["members"] = members
        standard_json = self.translate_curate_json_class.build_json_from_curate_json(curate_json, "root")
        i = 0
        while i < 3:
            i += 1  # prevent endless loop
            self._append_child_nodes(standard_json, members)
            if self._count_unprocessed_members(members, (i > 1)) == 0:
                break
        if self.save_standard_json_locally:
            with open(id + ".json", "w") as output_file:
                json.dump(standard_json, output_file, indent=2, ensure_ascii=False)
        if self.save_curate_json_locally:
            with open(id + "_curate.json", "w") as output_file:
                json.dump(curate_json, output_file, indent=2, ensure_ascii=False)
        return standard_json

    def _get_ancestry_list(self, ancestry_array):
        """ This is always an array containing a single string separated by slash (/) """
        ancestry_string = ancestry_array[0]
        ancestry_string = ancestry_string.replace("und:", "")
        ancestry_list = ancestry_string.split("/")
        return ancestry_list

    def _append_child_nodes(self, standard_json, members):
        parent_id = standard_json["id"]
        for member in members:
            for member_key, member_value in member.items():
                ancestry_list = self._get_ancestry_list(member_value.get("partOf", []))
                parent_node = self._get_parent_node(standard_json, ancestry_list)
                if parent_node and not member_value.get("processed", False):
                    child_json = self.translate_curate_json_class.build_json_from_curate_json(member_value)
                    child_json["collectionId"] = parent_node["collectionId"]
                    parent_id = parent_node["id"]
                    child_json["parentId"] = parent_id
                    child_json["sequence"] = self._accumulate_sequences_by_parent(parent_id)
                    self._fix_child_file_info(child_json)
                    if "children" not in parent_node:
                        parent_node["children"] = []
                    parent_node["children"].append(child_json)
                    member_value["processed"] = True
        return standard_json

    def _get_parent_node(self, json_object, ancestry_list, i=1):
        """ Last element of ancestry_list is item to be saved """
        parent_node = {}
        local_ancestry_list = list(ancestry_list)
        if len(local_ancestry_list) > 1:
            i += 1
            # print("loop within _get_parent_node = ", i, ancestry_list)
            parent_to_find = local_ancestry_list.pop(0)
            if json_object["id"] == parent_to_find:
                parent_node = json_object
            elif "children" in json_object:
                # print("children = ", parent_to_find, json_object)
                for child in json_object["children"]:
                    if child.get("id", "") == parent_to_find:
                        parent_node = child
                        break
            if parent_node and len(local_ancestry_list) > 1 and i < 5:  # early out if nested unreasonably deeply
                parent_node = self._get_parent_node(parent_node, local_ancestry_list, i)
        return parent_node

    def _fix_child_file_info(self, child_json):
        if "children" in child_json:
            for file_info in child_json["children"]:
                if file_info.get("workType") == "GenericFile":
                    file_info["collectionId"] = child_json["collectionId"]
                    file_info["parentId"] = child_json["id"]
                    file_info["thumbnail"] = (file_info.get("downloadUrl", "") == child_json.get("representativeImage", ""))
        return

    def _count_unprocessed_members(self, members, report_missing_members=False):
        count_unprocessed = 0
        for member in members:
            for member_key, member_value in member.items():
                if not member_value.get("processed", False):
                    count_unprocessed += 1
                    if report_missing_members:
                        print("Member did not process: ", member_key, member_value.get("partOf", ""))
        return count_unprocessed

    def get_members_list(self, url, parent_id):
        """ Call API to return members of a collection (or sub-collection)
            Note: query is of the form:  curate-server-base-url + "/api/items?part_of=<pid>" """
        results = []
        url = url + "&rows=100"  # Curate API prohibits returning more than 100 rows, so let's try to retrieve the max possible
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
                        # break  # early out to speed testing
                        i += 1
                        # if i > 3:
                        #     break
            url = self._get_next_page_url(member_results)
        return results

    def _get_members_details(self, members_json):
        for member in members_json:
            for key, value in member.items():
                if "itemUrl" in value:
                    details_json = self._get_json_given_url(value["itemUrl"])
                    for details_key, details_value in details_json.items():
                        value[details_key] = details_value
        return members_json

    def _get_next_page_url(self, json_member_results):
        url = None
        if "pagination" in json_member_results:
            url = json_member_results["pagination"].get("nextPage", None)
        return url

    def _get_json_given_url(self, url):
        """ Return json from URL."""
        json_response = {}
        print("calling url =", url, int(time.time() - self.start_time), 'seconds.')
        try:
            json_response = dependencies.requests.get(url, headers=self.curate_header).json()
        except ConnectionRefusedError:
            capture_exception('Connection refused on url ' + url)
        except:  # noqa E722 - intentionally ignore warning about bare except
            capture_exception('Error caught trying to process url ' + url)
        return json_response

    def _accumulate_sequences_by_parent(self, my_parent_id):
        sequence_to_use = 1
        if my_parent_id in self.sequences_within_parent:
            sequence_to_use = self.sequences_within_parent[my_parent_id] + 1
        self.sequences_within_parent[my_parent_id] = sequence_to_use
        return sequence_to_use
