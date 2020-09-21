# test_collections_api.py
""" test collections_api """
import _set_path  # noqa  # pylint: disable=unused-import
import unittest  # pylint: disable=wrong-import-order
from unittest.mock import patch  # pylint: disable=wrong-import-order
import json  # pylint: disable=wrong-import-order
import os  # pylint: disable=wrong-import-order
from pathlib import Path  # pylint: disable=wrong-import-order
from dependencies.pipelineutilities.pipeline_config import setup_pipeline_config  # pylint: disable=wrong-import-order
from collections_api import CollectionsApi  # pylint: disable=wrong-import-order


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        self.event = {"local": True}
        self.event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
        self.config = setup_pipeline_config(self.event)
        self.collections_api_class = CollectionsApi(self.config)

    # @patch('s3_helpers.read_s3_json')
    # def test_01_get_id(self, mock_read_s3_json):
    #     """I'm having problems mocking calls to read_s3_json because it is neither a function in the collections_api_class that I am testing
    #         Nor is it a class so I could test it like in museum_export/test_process_web_kiosk_json_metadata"""
    #     mock_read_s3_json.return_value = {}
    #     actual_results = self.collections_api_class._get_id('1934.007.001')
    #     mock_read_s3_json.assert_called_once_with("marble-manifest-prod-processbucket-13bond538rnnb", "json/1934.007.001.json")
    #     self.assertEqual(actual_results, {})

    @patch('collections_api.CollectionsApi._get_id')
    def test_01_get_item_details(self, mock_get_id):
        """ test_01_get_item_details """
        filename = os.path.join(local_folder, 'test/1934.007.001_get_id.json')
        with open(filename, 'r') as input_source:
            mock_get_id.return_value = json.load(input_source)
        filename = os.path.join(local_folder, 'test/1934.007.001_get_item_details.json')
        actual_results = self.collections_api_class._get_item_details('1934.007.001')  # pylint: disable=protected-access
        # with open(filename, 'w') as f:
        #     json.dump(actual_results, f, indent=2)
        with open(filename, 'r') as input_source:
            expected_results = json.load(input_source)
        self.assertEqual(actual_results, expected_results)

    @patch('collections_api.CollectionsApi._get_id')
    def test_02_get_collection_details(self, mock_get_id):
        """ test_02_get_collection_details """
        filename = os.path.join(local_folder, 'test/1934.007.001_get_id.json')
        with open(filename, 'r') as input_source:
            mock_get_id.return_value = json.load(input_source)
        collection_list = ["1934.007.001"]
        actual_results = self.collections_api_class._get_collection_details(collection_list)  # pylint: disable=protected-access
        filename = os.path.join(local_folder, 'test/1934.007.001_collection_details.json')
        # with open(filename, 'w') as file:
        #     json.dump(actual_results, file, indent=2)
        with open(filename, 'r') as input_source:
            expected_results = json.load(input_source)
        self.assertEqual(actual_results, expected_results)

    @patch('collections_api.CollectionsApi._call_get_matching_s3_objects')
    def test_03_get_collection_list(self, mock_get_matching_s3_objects):
        """ test_03_get_collection_list """
        filename = os.path.join(local_folder, 'test/matching_s3_objects.json')
        with open(filename, 'r') as input_source:
            mock_get_matching_s3_objects.return_value = json.load(input_source)
        actual_results = self.collections_api_class._get_collection_list('embark')  # pylint: disable=protected-access
        filename = os.path.join(local_folder, 'test/embark_collections_list.json')
        # with open(filename, 'w') as file:
        #     json.dump(actual_results, file, indent=2)
        with open(filename, 'r') as input_source:
            expected_results = json.load(input_source)
        self.assertEqual(actual_results, expected_results)


class SaveDataForTests():
    """ Class for test fixtures """
    def __init__(self):
        self.event = {"local": True}
        self.event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
        self.config = setup_pipeline_config(self.event)
        self.collections_api_class = CollectionsApi(self.config)

    def save_data(self):
        """ save_data """
        item_id = "1934.007.001"
        self.save_get_id(item_id)
        self.save_get_item_details(item_id)
        self.save_get_collection_list("embark")

    def save_get_id(self, item_id):
        """ save_get_id """
        item_json = self.collections_api_class._get_id(item_id)  # pylint: disable=protected-access
        with open(os.path.join(local_folder, 'test/' + item_id + '_get_id.json'), 'w') as file:
            json.dump(item_json, file, indent=2)

    def save_get_item_details(self, item_id):
        """ save_get_item_details """
        item_details_json = self.collections_api_class._get_item_details(item_id)  # pylint: disable=protected-access
        with open(os.path.join(local_folder, 'test/' + item_id + '_get_item_details.json'), 'w') as file:
            json.dump(item_details_json, file, indent=2)

    def save_get_collection_list(self, source):
        """ save_get_collection_list """
        collection_list = self.collections_api_class._get_collection_list(source)  # pylint: disable=protected-access
        with open(os.path.join(local_folder, 'test/' + source + '_collections_list.json'), 'w') as file:
            json.dump(collection_list, file, indent=2)
        collection_details = self.collections_api_class._get_collection_details(collection_list)  # pylint: disable=protected-access
        with open(os.path.join(local_folder, 'test/' + source + '_collection_details.json'), 'w') as file:
            json.dump(collection_details, file, indent=2)


def save_data_for_tests():
    """ This must be called with aws vault activated """
    save_data_for_tests_class = SaveDataForTests()
    save_data_for_tests_class.save_data()


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    # save_data_for_tests()
    suite()
    unittest.main()
