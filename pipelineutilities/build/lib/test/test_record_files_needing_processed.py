# test_record_files_needing_processed.py
""" test record_files_needing_processed """
import _set_path  # noqa
import unittest
import json
import os
from pathlib import Path
from datetime import datetime
from pipelineutilities.record_files_needing_processed import FilesNeedingProcessed
from pipelineutilities.pipeline_config import setup_pipeline_config


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        self.event = {"local": True}
        self.event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
        self.config = setup_pipeline_config(self.event)
        self.files_needing_processed_class = FilesNeedingProcessed(self.config)
        filename = local_folder + 'zp38w953h0s_standard.json'
        with open(filename, 'r') as input_source:
            self.standard_json = json.load(input_source)

    def test_01_default_export_since_date(self):
        actual_results = self.files_needing_processed_class._default_export_since_date()
        self.assertTrue(isinstance(actual_results, datetime))

    def test_02_record_has_been_updated_recently(self):
        export_since_date = self.files_needing_processed_class._default_export_since_date()
        standard_json = {"createdDate": "2018-11-16T00:00:00Z", "modifiedDate": "2019-11-16T00:00:00Z"}
        actual_results = self.files_needing_processed_class._record_has_been_updated_recently(standard_json, export_since_date)
        self.assertFalse(actual_results)
        export_since_date = datetime.strptime("2019-11-16T00:00:00Z", '%Y-%m-%dT%H:%M:%SZ')
        actual_results = self.files_needing_processed_class._record_has_been_updated_recently(standard_json, export_since_date)
        self.assertTrue(actual_results)
        standard_json = {"createdDate": "2018-11-16T00:00:00Z"}
        actual_results = self.files_needing_processed_class._record_has_been_updated_recently(standard_json, export_since_date)
        self.assertFalse(actual_results)
        export_since_date = datetime.strptime("2018-11-16T00:00:00Z", '%Y-%m-%dT%H:%M:%SZ')
        actual_results = self.files_needing_processed_class._record_has_been_updated_recently(standard_json, export_since_date)
        self.assertTrue(actual_results)

    def test_03_create_files_dict(self):
        results = self.files_needing_processed_class._create_files_dict(self.standard_json, True, "")
        with open(local_folder + 'sample_image_dict.json', 'w') as f:
            json.dump(results, f, indent=2)
        # results = self.files_needing_processed_class.accumulated_filess_list
        self.assertTrue(len(results["curate"]) == 197)

    def test_04_create_files_dict(self):
        # Because I know that the modified date in the file is 2020-1-29 for every record, I will test against that date
        self.files_needing_processed_class.accumulated_filess_list = []
        export_since_date = datetime.strptime("2020-1-29T00:00:00Z", '%Y-%m-%dT%H:%M:%SZ')
        results = self.files_needing_processed_class._create_files_dict(self.standard_json, False, export_since_date)
        # results = self.files_needing_processed_class.accumulated_filess_list
        self.assertTrue(len(results["curate"]) == 197)

    def test_05_create_files_dict(self):
        # Because I know that the modified date in the file is 2020-1-29 for every file record, I will test against 2020-1-30
        self.files_needing_processed_class.accumulated_filess_list = []
        export_since_date = datetime.strptime("2020-1-30T00:00:00Z", '%Y-%m-%dT%H:%M:%SZ')
        results = self.files_needing_processed_class._create_files_dict(self.standard_json, False, export_since_date)
        # results = self.files_needing_processed_class.accumulated_filess_list
        self.assertTrue(len(results) == 0)

    def test_06_get_key_given_file_path(self):
        actual_results = self.files_needing_processed_class._get_key_given_file_path("https://drive.google.com/a/nd.edu/file/d/1JHUNou4Z1izI6C-X2Tw2ZgUKN8eAOJAY/view")
        expected_results = "google"
        self.assertTrue(actual_results == expected_results)
        actual_results = self.files_needing_processed_class._get_key_given_file_path("https://curate.nd.edu/api/items/download/pz50gt57r3h")
        expected_results = "curate"
        self.assertTrue(actual_results == expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
