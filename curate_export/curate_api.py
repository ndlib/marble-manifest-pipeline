# curate_api.py
import time
import json
from datetime import datetime
import dependencies.requests
from dependencies.sentry_sdk import capture_exception
from translate_curate_json_node import TranslateCurateJsonNode
from csv_from_json import CsvFromJson
from save_csv import SaveCsv
from create_standard_json import CreateStandardJson


class CurateApi():
    """ This performs all Curate API processing """
    def __init__(self, config, event, time_to_break):
        self.config = config
        self.event = event
        self.curate_header = {"X-Api-Token": self.config["curate-token"]}
        self.start_time = time.time()
        self.time_to_break = time_to_break
        self.translate_curate_json_node_class = TranslateCurateJsonNode(config, event)
        self.save_standard_json_locally = event.get("local", False)
        self.save_curate_json_locally = event.get("local", False)
        self.create_standard_json_class = CreateStandardJson(config, event)
        self.csv_from_json_class = CsvFromJson(self.config.get("csv-field-names", []))
        self.save_csv_class = SaveCsv(config, event)

    def get_curate_items(self, ids):
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

    def get_curate_item(self, id):
        """ Get json metadata for a curate item given an item id
            Note: query is of the form: curate-server-base-url + "/api/items/<pid>" """
        standard_json = {}
        url = self.config["curate-server-base-url"] + "/api/items/" + id
        curate_json = self._get_json_given_url(url)
        members = []
        if "membersUrl" in curate_json:
            members = self._get_members_list(curate_json['membersUrl'], id)
            members = self._get_members_details(members)
            curate_json["members"] = members
        standard_json = self.translate_curate_json_node_class.build_json_from_curate_json(curate_json, "root")
        standard_json = self.create_standard_json_class.build_standard_json_tree(standard_json, members)
        csv_string = self.csv_from_json_class.return_csv_from_json(standard_json)
        self.save_csv_class.save_csv_file(id, csv_string)
        if self.save_standard_json_locally:
            with open(id + ".json", "w") as output_file:
                json.dump(standard_json, output_file, indent=2, ensure_ascii=False)
        if self.save_curate_json_locally:
            with open(id + "_curate.json", "w") as output_file:
                json.dump(curate_json, output_file, indent=2, ensure_ascii=False)
        return standard_json

    def _get_members_list(self, url, parent_id):
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
                        i += 1
                        # if i > 3:  # early out for testing
                        #     break
            url = self._get_next_page_url(member_results)
        return results

    def _get_members_details(self, members_json):
        """ For each member, do an API call to get all metadata details we know about. """
        for member in members_json:
            for key, value in member.items():
                if "itemUrl" in value:
                    details_json = self._get_json_given_url(value["itemUrl"])
                    for details_key, details_value in details_json.items():
                        value[details_key] = details_value
        return members_json

    def _get_next_page_url(self, json_member_results):
        """ For results with pagination, get the next url to be processed """
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
