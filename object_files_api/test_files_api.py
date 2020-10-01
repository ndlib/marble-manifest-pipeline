# test_files_api.py
""" test files_api """
import _set_path  # noqa
import unittest
import json
import os
import io
from datetime import datetime
from dateutil.tz import tzutc
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
        self.files_api_class = FilesApi(self.event, self.config)

    def test_04_crawl_available_files_from_s3_or_cache(self):
        actual_results = self.files_api_class._crawl_available_files_from_s3_or_cache(False)
        with io.open(os.path.join(os.path.dirname(__file__), 'test/crawl_available_files_cache.json'), 'r', encoding='utf-8') as json_file:
            expected_results = json.load(json_file)
        self.assertEqual(actual_results, expected_results)

    def test_05_convert_directory_to_json(self):
        all_directories = self.files_api_class._crawl_available_files_from_s3_or_cache(False)
        for key, value in all_directories.items():
            actual_results = self.files_api_class._convert_directory_to_json(value)
            filename = os.path.join(os.path.dirname(__file__), 'test', key.replace('/', '-')[1:] + '.json')
            # with open(filename, 'w') as f:
            #     json.dump(actual_results, f, indent=2)
            print(filename)
            with io.open(filename, 'r', encoding='utf-8') as json_file:
                expected_results = json.load(json_file)
            break
        self.assertEqual(actual_results, expected_results)

    def test_06_convert_directory_to_json(self):
        directories = self.files_api_class._crawl_available_files_from_s3_or_cache()
        directory = '/collections/ead_xml/images/BPP_1001/BPP_1001-001'
        expected_results = {'id': 'collections-ead_xml-images-BPP_1001-BPP_1001-001', 'label': 'BPP_1001 001', 'lastModified': '2020-01-28T15:01:53+00:00', 'path': 'S3://rbsc-test-files/collections/ead_xml/images/BPP_1001', 'source': 'rbsc-test-files', 'sourceType': 'S3', 'uri': 'https://presentation-iiif.library.nd.edu/objectFiles/collections-ead_xml-images-BPP_1001-BPP_1001-001', 'numberOfFiles': 2}  # noqa: #501
        actual_results = self.files_api_class._convert_directory_to_json(directories[directory])
        filename = os.path.join(os.path.dirname(__file__), 'test', directory.replace('/', '-')[1:] + '.json')
        with io.open(filename, 'r', encoding='utf-8') as json_file:
            expected_results = json.load(json_file)
        self.assertEqual(2, expected_results['numberOfFiles'])
        self.assertEqual(actual_results, expected_results)

    def test_05_save_file_objects_per_collection(self):
        all_directories = self.files_api_class._crawl_available_files_from_s3_or_cache(False)
        actual_results = 0
        for _key, value in all_directories.items():
            actual_results = self.files_api_class._save_file_objects_per_collection(value, {})
            expected_results = len(value['files'])
            self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    # save_data_for_tests()
    suite()
    unittest.main()
