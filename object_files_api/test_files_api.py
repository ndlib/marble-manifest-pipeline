# test_files_api.py
""" test files_api """
import _set_path  # noqa
import unittest
import json
import os
import io
from pathlib import Path
from dependencies.pipelineutilities.pipeline_config import setup_pipeline_config
from files_api import FilesApi


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        self.event = {"local": True}
        self.event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
        self.config = setup_pipeline_config(self.event)
        self.config['test'] = True
        self.config['website-metadata-tablename'] = 'some_random_table_name'
        self.files_api_class = FilesApi(self.event, self.config)

    def test_01_crawl_available_files_from_s3_or_cache(self):
        """ test_01_crawl_available_files_from_s3_or_cache """
        actual_results = self.files_api_class._crawl_available_files_from_s3_or_cache(False)
        with io.open(os.path.join(os.path.dirname(__file__), 'test/crawl_available_files_cache.json'), 'r', encoding='utf-8') as json_file:
            expected_results = json.load(json_file)
        self.assertEqual(actual_results, expected_results)

    def test_02_save_file_objects_per_collection(self):
        """ test_02_save_file_objects_per_collection """
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
