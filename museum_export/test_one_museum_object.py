# test_find_google_images.py
""" test find_google_images """
import _set_path  # noqa
import unittest
from pathlib import Path
import json
import os
import io
import time
from datetime import datetime, timedelta
from process_one_museum_object import ProcessOneMuseumObject
from dependencies.pipelineutilities.pipeline_config import setup_pipeline_config  # noqa: E402

local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        self.event = {"local": True}
        self.event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
        self.config = setup_pipeline_config(self.event)
        self.time_to_break = datetime.now() + timedelta(seconds=self.config['seconds-to-allow-for-processing'])

    def test_1_get_embark_metadata_url(self):
        """ test_2 _get_embark_metadata_url """
        with io.open(local_folder + 'test/1934.007.001_image_files.json', 'r', encoding='utf-8') as json_file:
            image_file_info = json.load(json_file)
        with io.open(local_folder + 'test/1934.007.001_web_kiosk.json', 'r', encoding='utf-8') as json_file:
            web_kiosk_json = json.load(json_file)
        for _key, value in web_kiosk_json["objects"].items():
            museum_object = value
            break
        process_one_museum_object_class = ProcessOneMuseumObject(self.config, image_file_info, time.time())
        actual_results = process_one_museum_object_class.process_object(museum_object)
        filename = local_folder + 'test/1934.007.001_standard.json'
        # with open(filename, 'w') as f:
        #     json.dump(actual_results, f, indent=2)
        with io.open(filename, 'r', encoding='utf-8') as json_file:
            expected_results = json.load(json_file)
        actual_results = self.fix_file_created_date(actual_results, expected_results["fileCreatedDate"])
        self.assertTrue(actual_results == expected_results)

    def fix_file_created_date(self, standard_json: dict, file_created_date: str) -> dict:
        if "fileCreatedDate" in standard_json:
            standard_json["fileCreatedDate"] = file_created_date
        if "items" in standard_json:
            for item in standard_json["items"]:
                item = self.fix_file_created_date(item, file_created_date)
        return standard_json


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
