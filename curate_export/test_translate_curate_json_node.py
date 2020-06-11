# test_translate_curate_json_node.py
""" test translate_curate_json_node """
import _set_path  # noqa
import unittest
import json
import os
from pathlib import Path
from translate_curate_json_node import TranslateCurateJsonNode
from dependencies.pipelineutilities.pipeline_config import setup_pipeline_config


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        self.event = {"local": True}
        self.event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
        self.config = setup_pipeline_config(self.event)
        self.csv_field_names = self.config["csv-field-names"]
        self.translate_curate_json_node_class = TranslateCurateJsonNode(self.config)

    def test_01_get_value_from_curate_field(self):
        json_field_definition = {"label": "creators", "fields": ["contributor"], "extraProcessing": "format_creators", "format": "array"}
        curate_json = {"contributor": "Sorin, Edward"}
        actual_results = self.translate_curate_json_node_class._get_value_from_curate_field(json_field_definition, curate_json, {})
        expected_results = [{'fullName': 'Sorin, Edward', 'display': 'Sorin, Edward'}]
        # print("actual_results = ", actual_results)
        self.assertTrue(actual_results == expected_results)
        curate_json = {"contributor": ['Cavanaugh, John, 1870-1935', 'Ayscough, John, 1858-1928 (substituted for  Walsh, David I. (David Ignatius), 1872-1947']}
        actual_results = self.translate_curate_json_node_class._get_value_from_curate_field(json_field_definition, curate_json, {})
        expected_results = [
            {'fullName': 'Cavanaugh, John, 1870-1935', 'display': 'Cavanaugh, John, 1870-1935'},
            {'fullName': 'Ayscough, John, 1858-1928 (substituted for  Walsh, David I. (David Ignatius), 1872-1947', 'display': 'Ayscough, John, 1858-1928 (substituted for  Walsh, David I. (David Ignatius), 1872-1947'}  # noqa: E501
        ]
        # print("actual_results = ", actual_results)
        self.assertTrue(actual_results == expected_results)

    def test_02_get_json_node_value_from_curate(self):
        json_field_definition = {"label": "creators", "fields": ["contributor"], "extraProcessing": "format_creators", "format": "array"}
        curate_json = {"contributor": "Sorin, Edward"}
        actual_results = self.translate_curate_json_node_class._get_json_node_value_from_curate(json_field_definition, curate_json, {})
        expected_results = [{'fullName': 'Sorin, Edward', 'display': 'Sorin, Edward'}]
        # print("actual_results = ", actual_results)
        self.assertTrue(actual_results == expected_results)
        curate_json = {"contributor": ['Cavanaugh, John, 1870-1935', 'Ayscough, John, 1858-1928 (substituted for  Walsh, David I. (David Ignatius), 1872-1947']}
        actual_results = self.translate_curate_json_node_class._get_json_node_value_from_curate(json_field_definition, curate_json, {})
        expected_results = [
            {'fullName': 'Cavanaugh, John, 1870-1935', 'display': 'Cavanaugh, John, 1870-1935'},
            {'fullName': 'Ayscough, John, 1858-1928 (substituted for  Walsh, David I. (David Ignatius), 1872-1947', 'display': 'Ayscough, John, 1858-1928 (substituted for  Walsh, David I. (David Ignatius), 1872-1947'}  # noqa: E501
        ]
        # print("actual_results = ", actual_results)
        self.assertTrue(actual_results == expected_results)

    def test_03_build_json_from_curate_json(self):
        filename = local_folder + 'test/zp38w953h0s_curate.json'
        with open(filename, 'r') as input_source:
            curate_json = json.load(input_source)
        curate_member_json = curate_json["members"][0]
        for _member_key, member_value in curate_member_json.items():
            actual_results = self.translate_curate_json_node_class.build_json_from_curate_json(member_value, "root", {})
            break  # only get the first one
        # print("actual_results = ", actual_results)
        filename = local_folder + 'test/zp38w953h0s_one_node_nd.json'
        # with open(filename, "w") as output_file:
        #     json.dump(actual_results, output_file, indent=2, ensure_ascii=False)
        with open(filename, 'r') as input_source:
            expected_results = json.load(input_source)
        file_created_date_from_sample = expected_results.get("fileCreatedDate", "")
        actual_results = self.fix_file_created_date(actual_results, file_created_date_from_sample)
        self.assertTrue(actual_results == expected_results)

    def fix_file_created_date(self, json_object, file_created_date):
        json_object["fileCreatedDate"] = file_created_date
        if "items" in json_object:
            for item in json_object["items"]:
                item = self.fix_file_created_date(item, file_created_date)
        return json_object


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
