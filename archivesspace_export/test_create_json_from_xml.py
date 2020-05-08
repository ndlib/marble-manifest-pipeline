# test_create_json_from_xml.py
""" test create_json_from_xml """
import _set_path  # noqa
import os
import json
import unittest
from create_json_from_xml import createJsonFromXml
import xml.etree.ElementTree as ET


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """

    def test_1_test_creating_json_from_xml(self):
        """ test test_creating_json_from_xml """
        xml_record = ET.parse(local_folder + 'test/MSNEA8011_EAD.xml').getroot()
        create_json_from_xml_class = createJsonFromXml()
        nd_json = create_json_from_xml_class.get_nd_json_from_xml(xml_record)
        with open(local_folder + 'test/MSNEA8011_EAD.json', 'r') as input_source:
            expected_json = json.load(input_source)
        input_source.close()
        self.assertTrue(expected_json == nd_json)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
