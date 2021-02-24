# test_curate_api.py
""" test curate_api """
import _set_path  # noqa
import unittest
import json
import os
import io
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from get_curate_metadata import GetCurateMetadata
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
        self.get_curate_metadata_class = GetCurateMetadata(self.config, self.event, time_to_break)

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
        actual_results = self.get_curate_metadata_class._get_next_page_url(json_member_results)
        expected_results = "https://curate.nd.edu/api/items?page=2&part_of=zp38w953h0s&rows=100"
        self.assertTrue(actual_results == expected_results)

    @patch('get_curate_metadata.GetCurateMetadata._get_json_given_url')
    def test_02_get_members_list(self, mock_get_json_given_url):
        url = "https://curate.nd.edu/api/items?part_of=zp38w953h0s"
        parent_id = "zp38w953h0s"
        with io.open(local_folder + 'test/member_results_get_json_given_url_parent_zp38w953h0s.json', 'r', encoding='utf-8') as json_file:
            mock_get_json_given_url.return_value = json.load(json_file)
        rows_to_return = 100
        testing_mode = True
        actual_results = self.get_curate_metadata_class._get_members_list(url, parent_id, rows_to_return, testing_mode)
        file_name = local_folder + "test/" + parent_id + "_get_members_list.json"
        # with open(file_name, "w") as output_file:
        #     json.dump(actual_results, output_file, indent=2, ensure_ascii=False)
        with io.open(file_name, 'r', encoding='utf-8') as json_file:
            expected_results = json.load(json_file)
        # print("actual_results = ", actual_results)
        self.assertTrue(actual_results == expected_results)

    @patch('get_curate_metadata.GetCurateMetadata._get_json_given_url')
    def test_04_get_members_details(self, mock_get_json_given_url):
        parent_id = "zp38w953h0s"
        file_name = local_folder + "test/" + parent_id + "_get_members_list.json"
        with io.open(file_name, 'r', encoding='utf-8') as json_file:
            members_json = json.load(json_file)
        # members_json = [{'pv63fx74g23': {'id': 'pv63fx74g23', 'title': 'Notre Dame Commencement Program: August 2, 1845', 'type': 'Program', 'itemUrl': 'https://curate.nd.edu/api/items/pv63fx74g23', 'partOf': ['und:zp38w953h0s/und:pv63fx74g23']}}]  # noqa: E501
        testing_mode = True
        with io.open(os.path.join(local_folder, "test/pv63fx74g23_get_members_details_get_json_given_url.json"), 'r', encoding='utf-8') as json_file:
            mock_get_json_given_url.return_value = json.load(json_file)
        actual_results = self.get_curate_metadata_class._get_members_details(members_json, testing_mode)
        # print("actual_results = ", actual_results)
        filename = local_folder + 'test/expected_get_members_detail.json'
        # with open(filename, "w") as output_file:
        #     json.dump(actual_results, output_file, indent=2, ensure_ascii=False)
        with open(filename, 'r') as input_source:
            expected_results = json.load(input_source)
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
