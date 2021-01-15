# test_find_google_images.py
""" test find_google_images """
import _set_path  # noqa
import unittest
from unittest.mock import patch
from pathlib import Path
import json
import os
import io
from datetime import datetime, timedelta
from process_web_kiosk_json_metadata import ProcessWebKioskJsonMetadata
from dependencies.pipelineutilities.pipeline_config import setup_pipeline_config  # noqa: E402

local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class fake_images_for_mocking():
    def __init__(self, objects, google_credentials, drive_id):
        image_files_list = fake_images_for_mocking._get_image_files_list(objects)
        self.image_file_info = fake_images_for_mocking._find_images_for_all_objects(image_files_list, google_credentials, drive_id)

    @staticmethod
    def _find_images_for_all_objects(image_files_list: list, google_credentials: dict, drive_id: str) -> dict:
        image_files = {"abc": 123}
        return image_files

    @staticmethod
    def _get_image_files_list(objects: dict) -> list:
        image_files_list = []
        return image_files_list


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        self.event = {"local": True}
        self.event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
        self.config = setup_pipeline_config(self.event)
        self.time_to_break = datetime.now() + timedelta(seconds=self.config['seconds-to-allow-for-processing'])

    def test_01_get_embark_metadata_url(self):
        """ test_01 _get_embark_metadata_url """
        event = {"mode": "ids", "ids": ["1934.007.001"]}
        json_web_kiosk_class = ProcessWebKioskJsonMetadata(self.config, event, self.time_to_break)
        actual_results = json_web_kiosk_class._get_embark_metadata_url("ids", "1934.007.001")
        expected_results = "http://notredame.dom5182.com:8080/results.html?layout=marble&format=json&maximumrecords=-1&recordType=objects_1&query=Disp_Access_No=1934.007.001"
        self.assertEqual(actual_results, expected_results)

    @patch('process_web_kiosk_json_metadata.ProcessWebKioskJsonMetadata._get_metadata_given_url')
    def test_02_get_composite_json_for_all_named_ids(self, mock_get_metadata_given_url):
        """ test_02_get_composite_json_for_all_named_ids """
        event = {"mode": "ids", "ids": ["1934.007.001"], "local": True}
        with io.open(local_folder + 'test/1934.007.001_web_kiosk.json', 'r', encoding='utf-8') as json_file:
            composite_json = json.load(json_file)
        mock_get_metadata_given_url.return_value = composite_json
        json_web_kiosk_class = ProcessWebKioskJsonMetadata(self.config, event, self.time_to_break)
        actual_results = json_web_kiosk_class._get_composite_json_for_all_named_ids("ids")
        self.assertEqual(actual_results, composite_json)

    @patch('get_image_info_for_all_objects.GetImageInfoForAllObjects', fake_images_for_mocking)
    def test_03_find_images_for_composite_json_metadata(self):
        """ Testing find_images_for_composite_json_metadata by mocking a whole class,
            using fake_images_for_mocking to substitute for calling GetImageInfoForAllObjects. """
        event = {"mode": "ids", "ids": ["1934.007.001"], "local": True}
        with io.open(local_folder + 'test/1934.007.001_web_kiosk.json', 'r', encoding='utf-8') as json_file:
            composite_json = json.load(json_file)
        json_web_kiosk_class = ProcessWebKioskJsonMetadata(self.config, event, self.time_to_break)
        actual_results = json_web_kiosk_class.find_images_for_composite_json_metadata(composite_json)
        self.assertEqual(actual_results, {"abc": 123})


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
