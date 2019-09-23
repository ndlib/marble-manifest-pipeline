import unittest
import json
import os
import sys
from pathlib import Path
where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am)
sys.path.append(where_i_am + "/..")
from find_images_for_objects.find_images_for_objects import find_images_for_objects  # noqa: E402
from find_images_for_objects.get_config import get_config  # noqa: E402
from find_images_for_objects.google_utilities import establish_connection_with_google_api  # noqa: E402


class TestCreateManifest(unittest.TestCase):
    def setUp(self):
        self.config = get_config()
        pass

    def test_find_images_for_objects(self):
        self.setUp()
        google_credentials = self.config['google']['credentials']
        google_connection = establish_connection_with_google_api(google_credentials)
        objects_needing_processed = {}
        current_path = str(Path(__file__).parent.absolute())
        file_name = current_path + '/../example/recently_changed_objects_needing_processed/example_objects_needing_processed.json'  # noqa: 501
        if os.path.isfile(file_name):
            with open(file_name, encoding='utf-8') as data_file:
                objects_needing_processed = json.loads(data_file.read())
                data_file.close()
        objects_needing_processed_with_image_references = {}
        objects_needing_processed_with_image_references = find_images_for_objects(google_connection, self.config, objects_needing_processed)  # noqa: 501
        file_name = current_path + '/../example/recently_changed_objects_needing_processed/example_objects_needing_processed_with_image_references.json'  # noqa: 501
        benchmark_objects_needing_processed_with_image_references = {}
        if os.path.isfile(file_name):
            with open(file_name, encoding='utf-8') as data_file:
                benchmark_objects_needing_processed_with_image_references = json.loads(data_file.read())
                data_file.close()
        self.assertEqual(objects_needing_processed_with_image_references,
                         benchmark_objects_needing_processed_with_image_references)


if __name__ == '__main__':
    unittest.main()
