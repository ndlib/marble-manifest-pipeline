# test_files_api.py
""" test files_api """
import _set_path  # noqa
import unittest
from unittest.mock import patch
import json
import os
import io
from datetime import datetime
from pathlib import Path
from dependencies.pipelineutilities.pipeline_config import setup_pipeline_config
from files_api import FilesApi, _serialize_json, _get_expire_time


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        self.event = {"local": True}
        self.event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
        self.config = setup_pipeline_config(self.event)
        self.config['test'] = True
        self.files_api_class = FilesApi(self.event, self.config)

    def test_01_serialize_json(self):
        """ test_01_serialize_json """
        test_json = {"abc": datetime(2020, 10, 13, 9, 0, 0, 0)}
        expected_results = {'abc': '2020-10-13T09:00:00'}
        actual_results = _serialize_json(test_json)
        self.assertEqual(actual_results, expected_results)

    def test_02_get_expire_time(self):
        """ test_02_get_expire_time """
        actual_results = _get_expire_time(datetime(2020, 10, 13, 9, 0, 0, 0), 3)
        expected_results = 1602853200
        self.assertEqual(actual_results, expected_results)

    @patch('files_api._get_expire_time')
    def test_03_save_json_to_dynamo(self, mock_get_expire_time):
        """ test_03_save_json_to_dynamo """
        mock_get_expire_time.return_value = 1602853200
        test_json = {"abc": datetime(2020, 10, 13, 9, 0, 0, 0)}
        actual_results = self.files_api_class._save_json_to_dynamo(test_json)
        expected_results = False
        self.assertEqual(actual_results, expected_results)
        expected_altered_test_json = {'abc': '2020-10-13T09:00:00', 'expireTime': 1602853200}
        self.assertEqual(test_json, expected_altered_test_json)

    def test_04_crawl_available_files_from_s3_or_cache(self):
        """ test_04_crawl_available_files_from_s3_or_cache """
        actual_results = self.files_api_class._crawl_available_files_from_s3_or_cache(False)
        with io.open(os.path.join(os.path.dirname(__file__), 'test/crawl_available_files_cache.json'), 'r', encoding='utf-8') as json_file:
            expected_results = json.load(json_file)
        self.assertEqual(actual_results, expected_results)

    def test_05_save_file_objects_per_collection(self):
        all_directories = self.files_api_class._crawl_available_files_from_s3_or_cache(False)
        for _key, value in all_directories.items():
            actual_results = self.files_api_class._save_file_objects_per_collection(value)
            self.assertEqual(actual_results[0]['id'], actual_results[0]['key'])
            break


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    # save_data_for_tests()
    suite()
    unittest.main()
