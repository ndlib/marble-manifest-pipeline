""" Get Curate Metadata
    Decides whether to use stored Curate metadata or to harvest it now.
    Either way, returns complete curate_json """

import io
import json
import os
import time
import dependencies.requests
from dependencies.sentry_sdk import capture_exception
from save_json_to_dynamo import SaveJsonToDynamo
from s3_helpers import read_s3_json, write_s3_json


class GetCurateMetadata():
    """ This performs all Curate processing """

    def __init__(self, config, event, time_to_break):
        self.config = config
        self.event = event
        if not self.config.get('local', True):
            self.curate_header = {"X-Api-Token": self.config["curate-token"]}
        self.start_time = time.time()
        self.time_to_break = time_to_break
        self.save_curate_json_locally = event.get("local", False)
        self.local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"
        self.attempting_huge_export_with_resumption_flag = False
        self.save_json_to_dynamo_class = SaveJsonToDynamo(config, self.config.get('website-metadata-tablename', ''))

    def get_curate_json(self, item_id: str) -> dict:
        """ Decide whether to use saved curate json or whether to get it ourselves, returning curate_json"""
        curate_root_json = self._get_root_item_from_curate(item_id)
        if not curate_root_json:
            return {}
        date_from_curate = curate_root_json.get('dateSubmitted')[:10]
        saved_curate_json = self._get_saved_curate_json(item_id)
        date_from_saved_curate_json = saved_curate_json.get('dateSubmitted', '')[:10]
        if not date_from_saved_curate_json or date_from_curate > date_from_saved_curate_json:
            print('retrieving fresh content from Curate.  Saved file had date of', date_from_saved_curate_json, ', curate had date of', date_from_curate)
            curate_json = self._get_curate_json_from_curate(item_id)
            members = []
            if "membersUrl" in curate_json:
                members = self._get_members_json(curate_json, item_id)
                while self._more_unprocessed_members_exist(members):
                    members = self._get_members_details(members, False, item_id, 20)
                    curate_json["members"] = members
                    if self.save_curate_json_locally or self.attempting_huge_export_with_resumption_flag:
                        with open(self.local_folder + "test/" + item_id + "_curate.json", "w") as output_file:
                            json.dump(curate_json, output_file, indent=2, ensure_ascii=False)
                self._save_curate_json(curate_json)
        else:
            print("using saved_curate_json")
            curate_json = saved_curate_json
        return curate_json

    def _get_root_item_from_curate(self, item_id: str) -> dict:
        """ Retrieve the root item for item_id from Curate """
        curate_json = {}
        if not self.config.get('local', True):
            url = self.config["curate-server-base-url"] + "/api/items/" + item_id
            curate_json = self._get_json_given_url(url)
        return curate_json

    def _get_curate_json_from_curate(self, item_id: str) -> dict:
        """ If self.attempting_huge_export_with_resumption_flag, read file locally
                (if it exists), otherwise, get json using url to Curate API """
        url = self.config["curate-server-base-url"] + "/api/items/" + item_id
        filename = self.local_folder + "test/" + item_id + "_curate.json"
        if self.attempting_huge_export_with_resumption_flag and os.path.exists(filename):
            with io.open(filename, 'r', encoding='utf-8') as json_file:
                curate_json = json.load(json_file)
        else:
            curate_json = self._get_json_given_url(url)
            if self.save_curate_json_locally or self.attempting_huge_export_with_resumption_flag:
                with open(filename, "w") as output_file:
                    json.dump(curate_json, output_file, indent=2, ensure_ascii=False)
        return curate_json

    def _get_saved_curate_json(self, item_id: str) -> dict:
        """ If original curate metadata has already been stored, use that instead of retrieving from Curate API
            If not running locally, check S3.  Else, check locally.  """
        if self.config.get('local', True):
            filename = os.path.join(self.local_folder, "save", item_id + "_curate.json")
            if os.path.exists(filename):
                with io.open(filename, 'r', encoding='utf-8') as json_file:
                    curate_json = json.load(json_file)
        else:
            key = os.path.join('save', item_id + '_curate.json')
            curate_json = read_s3_json(self.config['process-bucket'], key)
        return curate_json

    def _save_curate_json(self, curate_json: dict):
        """ Once we retrieve curate_json, save it so we can process more easily next time. """
        item_id = curate_json.get('id', '')
        if self.config.get('local', True):
            filename = os.path.join(self.local_folder, "save", item_id + "_curate.json")
            with open(filename, 'w') as f:
                json.dump(curate_json, f, indent=2)
        else:
            key = os.path.join('save', item_id + '_curate.json')
            write_s3_json(self.config['process-bucket'], key, curate_json)

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
                    if self.save_curate_json_locally and self.attempting_huge_export_with_resumption_flag:
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
