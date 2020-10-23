# test_create_json_from_xml.py
""" test add_paths_to_json_object """
import _set_path  # noqa
import os
import json
import unittest
from pipelineutilities.add_paths_to_json_object import AddPathsToJsonObject


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        pass

    def test_1_test_creating_json_from_xml(self):
        """ test test_creating_json_from_xml """
        with open(local_folder + './MSNEA8011_EAD_with_files.json', 'r') as input_source:
            standard_json = json.load(input_source)
        config = {}
        config['image-server-base-url'] = 'image-server-base-url'
        config['image-server-bucket'] = 'image-server-bucket'
        config['manifest-server-base-url'] = 'manifest-server-base-url'
        config['manifest-server-bucket'] = 'manifest-server-bucket'
        config['local'] = True
        add_paths_to_json_object_class = AddPathsToJsonObject(config)
        standard_json_with_files = add_paths_to_json_object_class.add_paths(standard_json)
        file_name = local_folder + './MSNEA8011_EAD_with_files_and_paths.json'
        # with open(file_name, 'w') as f:
        #     json.dump(standard_json_with_files, f, indent=2)
        with open(file_name, 'r') as input_source:
            expected_json = json.load(input_source)
        self.assertEqual(expected_json, standard_json_with_files)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
