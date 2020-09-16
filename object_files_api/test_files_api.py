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

    def test_01_convert_object_to_json(self):
        input = {"id": "abc"}
        expected_results = {"id": "abc", "uri": "https://presentation-iiif.library.nd.edu/objectFiles/abc"}
        actual_results = self.files_api_class._convert_object_to_json(input)
        self.assertEqual(actual_results, expected_results)

    def test_02_convert_object_to_json(self):
        directories = self.files_api_class._crawl_available_files_from_s3_or_cache()
        directory = '/collections/ead_xml/images/BPP_1001/BPP_1001-001'

        actual_results = self.files_api_class._convert_object_to_json(directories[directory]['files'][0])
        expected_results = {'eTag': '"fae679e8863fc9963e7a7e4d97290623"', 'fileId': '/collections/ead_xml/images/BPP_1001/BPP_1001-001', 'iiifImageFilePath': 's3://marble-data-broker-publicbucket-kebe5zkimvyg/collections/ead_xml/images/BPP_1001/BPP_1001-001-F2.jpg', 'iiifImageUri': 'https://image-iiif.libraries.nd.edu/iiif/2/collections/ead_xml/images/BPP_1001/BPP_1001-001-F2.jpg', 'key': 'collections/ead_xml/images/BPP_1001/BPP_1001-001-F2.jpg', 'label': 'https://rarebooks library nd edu F2', 'lastModified': '2020-01-28T15:01:53+00:00', 'path': 's3://rbsc-test-files/collections/ead_xml/images/BPP_1001/BPP_1001-001-F2.jpg', 'size': 2360533, 'source': 'rbsc-test-files', 'sourceType': 'S3', 'sourceUri': 'https://rarebooks.library.nd.edu/collections/ead_xml/images/BPP_1001/BPP_1001-001-F2.jpg', 'storageClass': 'STANDARD'}  # noqa: #501
        self.assertEqual(actual_results, expected_results)

    def test_03_fix_json_serial_problems(self):
        input = {'ETag': '"3b16c210f529e33c3acffce66bc2268d"'}
        input['LastModified'] = datetime(2020, 3, 2, 19, 35, 2, tzinfo=tzutc())
        actual_results = self.files_api_class._fix_json_serial_problems(input)
        expected_results = {'ETag': '"3b16c210f529e33c3acffce66bc2268d"', 'LastModified': '2020-03-02T19:35:02+00:00'}
        self.assertEqual(actual_results, expected_results)

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
