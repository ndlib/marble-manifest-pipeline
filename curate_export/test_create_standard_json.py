# test_create_standard_json.py
""" test create_standard_json """
import _set_path  # noqa
import unittest
import json
import os
from pathlib import Path
from create_standard_json import CreateStandardJson
from pipelineutilities.pipeline_config import setup_pipeline_config


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        self.event = {"local": True}
        self.event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
        self.config = setup_pipeline_config(self.event)
        self.create_standard_json_class = CreateStandardJson(self.config)
        filename = local_folder + 'test/zp38w953h0s_curate.json'
        with open(filename, 'r') as input_source:
            self.curate_json = json.load(input_source)
        filename = local_folder + 'test/zp38w953h0s_preliminary_standard.json'
        with open(filename, 'r') as input_source:
            self.preliminary_standard_json = json.load(input_source)

    def test_01_get_ancestry_list(self):
        ancestry_array = ["und:zp38w953h0s/und:rv042r40226"]
        actual_results = self.create_standard_json_class._get_ancestry_list(ancestry_array)
        expected_results = ['zp38w953h0s', 'rv042r40226']
        # print("actual_results = ", actual_results)
        self.assertTrue(actual_results == expected_results)

    def test_02_accumulate_sequences_by_parent(self):
        parent_id = "parent_1"
        actual_results = self.create_standard_json_class._accumulate_sequences_by_parent(parent_id)
        expected_results = 1
        self.assertTrue(actual_results == expected_results)
        actual_results = self.create_standard_json_class._accumulate_sequences_by_parent(parent_id)
        expected_results = 2
        self.assertTrue(actual_results == expected_results)
        parent_id = "parent_2"
        actual_results = self.create_standard_json_class._accumulate_sequences_by_parent(parent_id)
        expected_results = 1
        self.assertTrue(actual_results == expected_results)
        parent_id = "parent_1"
        actual_results = self.create_standard_json_class._accumulate_sequences_by_parent(parent_id)
        expected_results = 3
        self.assertTrue(actual_results == expected_results)

    def test_03_count_unprocessed_members(self):
        members = self.curate_json["members"]
        actual_results = self.create_standard_json_class._count_unprocessed_members(members, False)
        # print("actual_results = ", actual_results)
        expected_results = 197
        self.assertTrue(actual_results == expected_results)

    def test_04_get_parent_node(self):
        ancestry_list = ['zp38w953h0s', 'rv042r40226']
        actual_results = self.create_standard_json_class._get_parent_node(self.preliminary_standard_json, ancestry_list)
        expected_results = self.preliminary_standard_json
        # print("actual_results = ", actual_results)
        self.assertTrue(actual_results == expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
