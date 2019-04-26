# test_create_new_index_record.py
""" test create_new_index_record """

import unittest
import json
from create_new_index_record import create_new_index_record
from xml.etree.ElementTree import ElementTree, tostring
from write_index_file import write_index_file


def get_json_input(filename):
    """ get pre-formed json to make sure testing is uniform """
    with open(filename, 'r') as input_source:
        json_input = json.load(input_source)
    input_source.close()
    return json_input


class Test(unittest.TestCase):
    """ Class for test fixtures """

    def test_create_new_index_record(self):
        """test create_new_index_record"""
        json_input = get_json_input('./test/1934.007.001.json')
        self.assertTrue(json_input != '{}')
        # print(json_input)
        index_output = create_new_index_record(json_input)
        self.assertTrue(isinstance(index_output, ElementTree))
        index_directory = 'test/actual_results'
        filename = '1934.007.001.xml'
        write_index_file(index_directory, filename, index_output)
        actual_results_file_name = 'test/actual_results/1934.007.001.xml'
        expected_results_file_name = 'test/expected_results/1934.007.001.xml'
        actual_results = ElementTree(file=actual_results_file_name)
        expected_results = ElementTree(file=expected_results_file_name)
        # print(ElementTree.tostring(xml_tree.getroot()))
        self.assertTrue(tostring(actual_results.getroot()) == tostring(expected_results.getroot()))


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
