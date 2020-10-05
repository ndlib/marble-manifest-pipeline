# test_create_json_from_xml.py
""" test create_json_from_xml """
import _set_path  # noqa
import os
import json
import unittest
from pipelineutilities.add_files_to_json_object import AddFilesToJsonObject


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        pass

    def test_1_test_creating_json_from_xml(self):
        """ test test_creating_json_from_xml """
        with open(local_folder + '/MSNEA8011_EAD.json', 'r') as input_source:
            standard_json = json.load(input_source)
        config = {}
        config['local'] = True
        # To re-create copy of hash_of_available_files, uncomment the following lines and run in libnd
        # config['rbsc-image-bucket'] = "libnd-smb-rbsc"
        # config['local'] = False
        # config['image-server-base-url'] = "image-server-base-url"
        # config['image-server-bucket'] = "image-server-bucket"
        add_file_to_json_object_class = AddFilesToJsonObject(config)
        standard_json_with_files = add_file_to_json_object_class.add_files(standard_json)
        # with open(local_folder + '/MSNEA8011_EAD_with_files.json', 'w') as f:
        #     json.dump(standard_json_with_files, f, indent=2)
        with open(local_folder + '/MSNEA8011_EAD_with_files.json', 'r') as input_source:
            expected_json = json.load(input_source)

        self.assertEqual(expected_json, standard_json_with_files)

    def test_2_file_exists_in_list(self):
        files_list = [{"id": "MSN-COL_8501-05.b.150.jpg"}, {"id": "MSN-COL_8501-05.a.150.jpg"}]
        config = {}
        config['local'] = True
        add_file_to_json_object_class = AddFilesToJsonObject(config)
        self.assertTrue(add_file_to_json_object_class._file_exists_in_list(files_list, "MSN-COL_8501-05.b.150.jpg"))
        self.assertFalse(add_file_to_json_object_class._file_exists_in_list(files_list, "abc.txt"))

    def test_3_remove_existing_file_from_list(self):
        files_list = [{"id": "MSN-COL_8501-05.b.150.jpg"}, {"id": "MSN-COL_8501-05.a.150.jpg"}]
        config = {}
        config['local'] = True
        add_file_to_json_object_class = AddFilesToJsonObject(config)
        add_file_to_json_object_class._remove_existing_file_from_list(files_list, "MSN-COL_8501-05.b.150.jpg")
        self.assertEqual(files_list, [{"id": "MSN-COL_8501-05.a.150.jpg"}])


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
