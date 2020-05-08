# test_create_json_from_xml.py
""" test create_json_from_xml """
import _set_path  # noqa
import os
import json
import unittest
from add_files_to_nd_json import AddFilesToNdJson
import xml.etree.ElementTree as ET


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        with open(local_folder + 'test/hash_of_available_files.json', 'r') as input_source:
            self.hash_of_available_files = json.load(input_source)

    def test_1_test_creating_json_from_xml(self):
        """ test test_creating_json_from_xml """
        xml_record = ET.parse(local_folder + 'test/MSNEA8011_EAD.xml').getroot()
        config = {}
        config['rbsc-image-bucket'] = "libnd-smb-rbsc"
        add_files_to_nd_json_class = AddFilesToNdJson(config)
        add_files_to_nd_json_class.hash_of_available_files = self.hash_of_available_files
        nd_json_with_files = add_files_to_nd_json_class.add_files(xml_record)
        # with open(local_folder + 'test/MSNEA8011_EAD_with_files.json', 'w') as f:
        #     json.dump(nd_json_with_files, f, indent=2)
        with open(local_folder + 'test/MSNEA8011_EAD_with_files.json', 'r') as input_source:
            expected_json = json.load(input_source)
        input_source.close()
        self.assertTrue(expected_json == nd_json_with_files)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
