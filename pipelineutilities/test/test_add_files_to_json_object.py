# test_create_json_from_xml.py
""" test create_json_from_xml """
import _set_path  # noqa
import os
import json
import unittest
from pipelineutilities.add_files_to_json_object import AddFilesToJsonObject, change_file_extensions_to_tif


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        pass

    def test_01_test_creating_json_from_xml(self):
        """ test test_creating_json_from_xml """
        with open(local_folder + '/MSNEA8011_EAD.json', 'r') as input_source:
            standard_json = json.load(input_source)
        config = {}
        config['local'] = True
        # To re-create copy of hash_of_available_files, uncomment the following lines, connect aws vault to libnd, and run in libnd
        # Also, uncomment lines 16-18 in add_files_to_json_object.py
        # re-comment when done
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

    def test_02_file_exists_in_list(self):
        """ test_02_file_exists_in_list """
        files_list = [{"id": "MSN-COL_8501-05.b.150.jpg"}, {"id": "MSN-COL_8501-05.a.150.jpg"}]
        config = {}
        config['local'] = True
        add_file_to_json_object_class = AddFilesToJsonObject(config)
        self.assertTrue(add_file_to_json_object_class._file_exists_in_list(files_list, "MSN-COL_8501-05.b.150.jpg"))
        self.assertFalse(add_file_to_json_object_class._file_exists_in_list(files_list, "abc.txt"))

    def test_03_remove_existing_file_from_list(self):
        """test_03_remove_existing_file_from_list"""
        files_list = [{"id": "MSN-COL_8501-05.b.150.jpg"}, {"id": "MSN-COL_8501-05.a.150.jpg"}]
        config = {}
        config['local'] = True
        add_file_to_json_object_class = AddFilesToJsonObject(config)
        add_file_to_json_object_class._remove_existing_file_from_list(files_list, "MSN-COL_8501-05.b.150.jpg")
        self.assertEqual(files_list, [{"id": "MSN-COL_8501-05.a.150.jpg"}])

    def test_04_change_file_extensions_to_tif(self):
        """ test_04_change_file_extensions_to_tif """
        config = {}
        config['local'] = True
        each_file_dict = {
            "id": "1995.033.001/1995.033.001.a/1995_033_001_a-v0004.jpg",
            "title": "1995_033_001_a-v0004.jpg",
            "description": "1995_033_001_a-v0004.jpg",
            "mimeType": "image/jpeg",
            "filePath": "1995.033.001/1995.033.001.a/1995_033_001_a-v0004.jpg"
        }
        actual_results = change_file_extensions_to_tif(each_file_dict, ['.pdf'])
        expected_results = {
            'id': '1995.033.001/1995.033.001.a/1995_033_001_a-v0004.tif',
            'title': '1995_033_001_a-v0004.tif',
            'description': '1995_033_001_a-v0004.tif',
            'mimeType': 'image/jpeg',
            'filePath': '1995.033.001/1995.033.001.a/1995_033_001_a-v0004.tif'
        }
        self.assertEqual(actual_results, expected_results)

    def test_05_change_file_extensions_to_tif(self):
        """ test_05_change_file_extensions_to_tif """
        config = {}
        config['local'] = True
        each_file_dict = {
            "id": "https://rarebooks.library.nd.edu/digital/bookreader/MSN-EA_8011-1-B/images/MSN-EA_8011-01-B-000a.jpg"
        }
        actual_results = change_file_extensions_to_tif(each_file_dict, ['.pdf'])
        expected_results = {
            'id': 'https://rarebooks.library.nd.edu/digital/bookreader/MSN-EA_8011-1-B/images/MSN-EA_8011-01-B-000a.tif'
        }
        self.assertEqual(actual_results, expected_results)

    def test_06_change_file_extensions_to_tif(self):
        """ test_06_change_file_extensions_to_tif """
        config = {}
        config['local'] = True
        each_file_dict = {
            "id": "https://rarebooks library nd edu/digital/bookreader/MSN EA 8011 1 B/images/ B 000a"
        }
        actual_results = change_file_extensions_to_tif(each_file_dict, ['.pdf'])
        expected_results = {
            'id': 'https://rarebooks library nd edu/digital/bookreader/MSN EA 8011 1 B/images/ B 000a'
        }
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
