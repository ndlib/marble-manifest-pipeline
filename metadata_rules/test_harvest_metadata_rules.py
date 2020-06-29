# test_harvest_metadata_rules.py
""" test harvest_metadata_rules """
import _set_path  # noqa
import unittest
from unittest.mock import patch
import json
import os
from pathlib import Path
from harvest_metadata_rules import HarvestMetadataRules
from dependencies.pipelineutilities.pipeline_config import setup_pipeline_config, load_config_ssm


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        self.event = {"local": True}
        self.event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
        self.config = setup_pipeline_config(self.event)
        if not self.event["local"]:
            google_config = load_config_ssm(self.config['google_keys_ssm_base'])
            self.config.update(google_config)
            self.google_credentials = json.loads(self.config["museum-google-credentials"])
        else:
            self.google_credentials = {}
        self.harvest_metadata_rules_class = HarvestMetadataRules(self.google_credentials, True)

    def test_01_get_json_from_sheet_contents_list(self):
        field_name_for_key = "standard json field name"
        columns_to_export = ["preferred name", "schema.org mapping", "element", "marble display name",
                             "required", "vra mapping", "standard json field name", "display order"]
        sheet_name = 'aleph'
        filename = local_folder + 'test/' + sheet_name + '_sheet_contents_list.json'
        with open(filename, 'r') as input_source:
            sheet_contents_list = json.load(input_source)
        actual_results = self.harvest_metadata_rules_class._get_json_from_sheet_contents_list(sheet_contents_list, columns_to_export, field_name_for_key)
        filename = local_folder + 'sites/marble/' + sheet_name + '.json'
        with open(filename, 'r') as input_source:
            expected_results = json.load(input_source)
        # print("actual_results = ", actual_results)
        self.assertTrue(actual_results == expected_results)

    @patch('harvest_metadata_rules.HarvestMetadataRules._get_list_of_sheet_names')
    def test_02_harvest_google_spreadsheet_info(self, mock_get_list_of_sheet_names):
        expected_results = ['Aleph', 'Curate']
        mock_get_list_of_sheet_names.return_value = expected_results
        harvest_metadata_rules_class = HarvestMetadataRules({}, True)
        actual_results = harvest_metadata_rules_class._get_list_of_sheet_names({}, "")
        self.assertTrue(actual_results == expected_results)

    @patch('harvest_metadata_rules.HarvestMetadataRules._get_contents_of_one_tab_list')
    def test_03_harvest_google_spreadsheet_info(self, mock_get_contents_of_one_tab_list):
        google_spreadsheet_id = "1gKUkoG921EW0AAa-9c58Yn3wGyx8UXLnlFCWHs3G7E4"
        google_credentials = {"user": "name"}
        sheet_names_list = ['Aleph']
        columns_to_export = ["preferred name", "schema.org mapping", "element", "marble display name",
                             "required", "vra mapping", "standard json field name", "display order"]
        field_name_for_key = "standard json field name"
        harvest_metadata_rules_class = HarvestMetadataRules(google_credentials, True)
        filename = local_folder + 'test/aleph_sheet_contents_list.json'
        with open(filename, 'r') as input_source:
            sheet_contents_list = json.load(input_source)
        mock_get_contents_of_one_tab_list.return_value = sheet_contents_list
        actual_results = harvest_metadata_rules_class._get_contents_of_spreadsheet('marble', google_spreadsheet_id, sheet_names_list, columns_to_export, field_name_for_key)
        mock_get_contents_of_one_tab_list.assert_called_with(google_spreadsheet_id, 'Aleph')
        filename = local_folder + 'sites/marble/aleph.json'
        with open(filename, 'r') as input_source:
            expected_tab_results = json.load(input_source)
        expected_results = {}
        expected_results["aleph"] = expected_tab_results
        self.assertTrue(actual_results == expected_results)

    @patch('harvest_metadata_rules.HarvestMetadataRules._get_list_of_sheet_names')
    @patch('harvest_metadata_rules.HarvestMetadataRules._get_contents_of_one_tab_list')
    def test_04_harvest_google_spreadsheet_info(self, mock_get_contents_of_one_tab_list, mock_get_sheet_names):
        google_spreadsheet_id = "1gKUkoG921EW0AAa-9c58Yn3wGyx8UXLnlFCWHs3G7E4"
        harvest_metadata_rules_class = HarvestMetadataRules({}, True)
        filename = local_folder + 'test/curate_sheet_contents_list.json'
        with open(filename, 'r') as input_source:
            sheet_contents_list = json.load(input_source)
        mock_get_contents_of_one_tab_list.return_value = sheet_contents_list
        mock_get_sheet_names.return_value = ['Curate']
        actual_results = harvest_metadata_rules_class.harvest_google_spreadsheet_info('marble')
        mock_get_contents_of_one_tab_list.assert_called_with(google_spreadsheet_id, 'Curate')
        filename = local_folder + 'sites/marble/curate.json'
        with open(filename, 'r') as input_source:
            expected_tab_results = json.load(input_source)
        expected_results = {}
        expected_results["curate"] = expected_tab_results
        self.assertTrue(actual_results == expected_results)

    def test_05_read_site_to_harvest_control_json(self):
        harvest_metadata_rules_class = HarvestMetadataRules({}, True)
        actual_results = harvest_metadata_rules_class._read_site_to_harvest_control_json("marble")
        filename = local_folder + 'marble.json'
        with open(filename, 'r') as input_source:
            expected_results = json.load(input_source)
        self.assertTrue(actual_results == expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
