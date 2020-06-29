# test_curate_api.py
""" test curate_api """
import _set_path  # noqa
import unittest
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from curate_api import CurateApi
from pipelineutilities.pipeline_config import setup_pipeline_config


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        self.event = {"local": True}
        self.event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
        self.config = setup_pipeline_config(self.event)
        self.config['curate-token'] = 'some_token'
        time_to_break = datetime.now() + timedelta(seconds=self.config['seconds-to-allow-for-processing'])
        self.curate_api_class = CurateApi(self.config, self.event, time_to_break)

    def test_01_get_next_page_url(self):
        json_member_results = {
            "pagination": {
                "itemsPerPage": 100,
                "totalResults": 198,
                "currentPage": 1,
                "firstPage": "https://curate.nd.edu/api/items?part_of=zp38w953h0s&rows=100",
                "lastPage": "https://curate.nd.edu/api/items?page=2&part_of=zp38w953h0s&rows=100",
                "nextPage": "https://curate.nd.edu/api/items?page=2&part_of=zp38w953h0s&rows=100"
            }
        }
        actual_results = self.curate_api_class._get_next_page_url(json_member_results)
        expected_results = "https://curate.nd.edu/api/items?page=2&part_of=zp38w953h0s&rows=100"
        self.assertTrue(actual_results == expected_results)

    def test_02_get_members_list(self):
        url = "https://curate.nd.edu/api/items?part_of=zp38w953h0s"
        parent_id = "zp38w953h0s"
        rows_to_return = 1
        testing_mode = True
        actual_results = self.curate_api_class._get_members_list(url, parent_id, rows_to_return, testing_mode)
        expected_results = [{'pv63fx74g23': {'id': 'pv63fx74g23', 'title': 'Notre Dame Commencement Program: August 2, 1845', 'type': 'Program', 'itemUrl': 'https://curate.nd.edu/api/items/pv63fx74g23', 'partOf': ['und:zp38w953h0s/und:pv63fx74g23']}}]  # noqa: #501
        # print("actual_results = ", actual_results)
        self.assertTrue(actual_results == expected_results)

    def test_03_get_json_given_url(self):
        url = "https://curate.nd.edu/api/items?part_of=zp38w953h0s&rows=1"
        actual_results = self.curate_api_class._get_json_given_url(url)
        expected_results = {'query': {'queryUrl': 'https://curate.nd.edu/api/items?part_of=zp38w953h0s&rows=1', 'queryParameters': {'part_of': 'zp38w953h0s', 'rows': '1'}}, 'results': [{'id': 'pv63fx74g23', 'title': 'Notre Dame Commencement Program: August 2, 1845', 'type': 'Program', 'itemUrl': 'https://curate.nd.edu/api/items/pv63fx74g23', 'partOf': ['und:zp38w953h0s/und:pv63fx74g23']}], 'pagination': {'itemsPerPage': 1, 'totalResults': 198, 'currentPage': 1, 'firstPage': 'https://curate.nd.edu/api/items?part_of=zp38w953h0s&rows=1', 'lastPage': 'https://curate.nd.edu/api/items?page=198&part_of=zp38w953h0s&rows=1', 'nextPage': 'https://curate.nd.edu/api/items?page=2&part_of=zp38w953h0s&rows=1'}}  # noqa: E501
        # print("actual_results = ", actual_results)
        self.assertTrue(actual_results == expected_results)

    def test_04_get_members_details(self):
        members_json = [{'pv63fx74g23': {'id': 'pv63fx74g23', 'title': 'Notre Dame Commencement Program: August 2, 1845', 'type': 'Program', 'itemUrl': 'https://curate.nd.edu/api/items/pv63fx74g23', 'partOf': ['und:zp38w953h0s/und:pv63fx74g23']}}]  # noqa: E501
        testing_mode = True
        actual_results = self.curate_api_class._get_members_details(members_json, testing_mode)
        # print("actual_results = ", actual_results)
        filename = local_folder + 'test/expected_get_members_detail.json'
        # with open(filename, "w") as output_file:
        #     json.dump(actual_results, output_file, indent=2, ensure_ascii=False)
        with open(filename, 'r') as input_source:
            expected_results = json.load(input_source)
        self.assertTrue(actual_results == expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
