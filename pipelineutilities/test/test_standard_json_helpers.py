""" test_standard_json_helpers """
import _set_path  # noqa: F401
import json
import os
from pipelineutilities.standard_json_helpers import _remove_brackets, _remove_trailing_punctuation, _clean_up_standard_json_strings, \
    _load_language_codes, _add_language_display, _clean_up_standard_json_recursive, _add_publishers_node, _add_objectFileGroupId, \
    _find_object_file_group_id, _find_default_file_path, _add_sequence
import unittest


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"

with open(local_folder + 'loc_term_85012415.json', 'r', encoding='utf-8') as json_file:
    loc_term_85012315 = json.load(json_file)


class Test(unittest.TestCase):
    """ Test expand_getty_aat_terms """

    def test_01_clean_up_description(self):
        """ test_01_clean_up_description """
        sample_description = "this is a [dumb] test"
        actual_results = _remove_brackets(sample_description)
        expected_results = "this is a dumb test"
        self.assertEqual(actual_results, expected_results)

    def test_02_remove_trailing_punctuation(self):
        """ test_02_remove_trailing_punctuation """
        strings_to_test = [
            "Folk songs -- Ireland -- Instrumental settings.",
            "test1,",
            "test2/",
            "test3:",
            "test4;",
            "test5",
            "test6|pipe",
            "pipe7| plus,",
            "multiple8 trailing punctuations./",
            "multiple||| pipes"
        ]
        expected_results_list = [
            "Folk songs -- Ireland -- Instrumental settings",
            "test1",
            "test2",
            "test3",
            "test4",
            "test5",
            "test6pipe",
            "pipe7 plus",
            "multiple8 trailing punctuations.",
            "multiple pipes"
        ]
        for i, string in enumerate(strings_to_test):
            actual_results = _remove_trailing_punctuation(string)
            expected_results = expected_results_list[i]
        self.assertEqual(actual_results, expected_results)

    def test_03_clean_up_standard_json_strings(self):
        """ test_03_clean_up_standard_json_strings"""
        standard_json = {
            "description": "something [really] dumb",
            "title": "test0,",
            "subjects": [{"display": "test1/"}, {"display": "test2."}],
            "contributors": [{"display": "test3:"}],
            "languages": ['english']
        }
        actual_results = _clean_up_standard_json_strings(standard_json)
        expected_results = {
            "description": "something really dumb",
            "title": "test0,",
            "subjects": [{"display": "test1"}, {"display": "test2"}],
            "contributors": [{"display": "test3"}],
            "languages": [{'display': 'English', 'alpha2': 'en', 'alpha3': 'eng'}]
        }
        self.assertEqual(actual_results, expected_results)

    def test_04_load_language_codes(self):
        """ test_04_load_language_codes """
        actual_results = _load_language_codes()
        self.assertIsInstance(actual_results, list)
        self.assertGreater(len(actual_results), 100)

    def test_05_add_language_display(self):
        """ test_05_add_language_display """
        languages = ['eng', {'display': 'French'}]
        actual_results = _add_language_display(languages)
        expected_results = [
            {'display': 'English', 'alpha2': 'en', 'alpha3': 'eng'},
            {'display': 'French'}
        ]
        self.assertEqual(actual_results, expected_results)

    def test_06_clean_up_standard_json_recursive(self):
        """ test_06_clean_up_standard_json_recursive"""
        standard_json = {
            "description": "something [really] dumb",
            "title": "test0/",
            "subjects": [{"display": "test1/"}, {"display": "test2."}],
            "contributors": [{"display": "test3:"}],
            "items": [
                {"creators": [{"display": "test4;"}]}
            ],
            "languages": ['english'],
            "publisher": {'publisherName': 'abc', 'publisherLocation': 'xyz'},
            "collections": [{'display': "Capt. Francis O'Neill Collection of Irish Studies (University of Notre Dame. Library)/"}]
        }
        actual_results = _clean_up_standard_json_recursive(standard_json)
        expected_results = {
            "description": "something really dumb",
            "title": "test0",
            "subjects": [{"display": "test1"}, {"display": "test2"}],
            "contributors": [{"display": "test3"}],
            "items": [
                {"creators": [{"display": "test4"}]}
            ],
            "languages": [{'display': 'English', 'alpha2': 'en', 'alpha3': 'eng'}],
            "publishers": [{'display': 'abc, xyz', 'publisherName': 'abc', 'publisherLocation': 'xyz'}],
            "collections": [{'display': "Capt. Francis O'Neill Collection of Irish Studies (University of Notre Dame. Library)"}]
        }
        self.assertEqual(actual_results, expected_results)

    def test_07_add_publishers_node(self):
        """ test_07_add_publishers_node"""
        standard_json = {
            "publisherName": "published by I. Willis.",
            "publisherLocation": "Dublin,"
        }
        actual_results = _add_publishers_node(standard_json)
        expected_results = [
            {
                "display": "published by I. Willis, Dublin",
                "publisherName": "published by I. Willis.",
                "publisherLocation": "Dublin,"
            }
        ]
        self.assertEqual(actual_results, expected_results)

    def test_08_add_objectFileGroupId(self):
        """ test_08_add_objectFileGroupId """
        with open(local_folder + '1976.046.json', 'r', encoding='utf-8') as json_file:
            standard_json = json.load(json_file)
        standard_json = {
            'id': 'abc',
            'level': 'manifest',
            'items': [
                {'level': 'file', 'filePath': 'https://rarebooks.library.nd.edu/digital/MARBLE-images/BOO_000297305/BOO_000297305_000001.tif'},
                {'level': 'file', 'filePath': 'https://drive.google.com/a/nd.edu/file/d/17BsDDtqWmozxHZD23HOvIuX8igpBH2sJ/view'}]
        }
        actual_results = _add_objectFileGroupId(standard_json)
        expected_results = {
            'id': 'abc',
            'level': 'manifest',
            'objectFileGroupId': 'BOO_000297305',
            'items': [
                {'level': 'file', 'filePath': 'https://rarebooks.library.nd.edu/digital/MARBLE-images/BOO_000297305/BOO_000297305_000001.tif'},
                {'level': 'file', 'filePath': 'https://drive.google.com/a/nd.edu/file/d/17BsDDtqWmozxHZD23HOvIuX8igpBH2sJ/view'}
            ]
        }
        self.assertEqual(actual_results, expected_results)

    def test_09_add_objectFileGroupId_museum(self):
        """ test_09_add_objectFileGroupId """
        with open(local_folder + '1976.046.json', 'r', encoding='utf-8') as json_file:
            standard_json = json.load(json_file)
        standard_json = {
            'id': '1234.567',
            'level': 'manifest',
            'items': [
                {'level': 'file', 'filePath': 'https://drive.google.com/a/nd.edu/file/d/17BsDDtqWmozxHZD23HOvIuX8igpBH2sJ/view', 'objectFileGroupId': '1234.567'}]
        }
        actual_results = _add_objectFileGroupId(standard_json)
        expected_results = {
            'id': '1234.567',
            'level': 'manifest',
            'objectFileGroupId': '1234.567',
            'items': [
                {'level': 'file', 'filePath': 'https://drive.google.com/a/nd.edu/file/d/17BsDDtqWmozxHZD23HOvIuX8igpBH2sJ/view', 'objectFileGroupId': '1234.567'}
            ]
        }
        self.assertEqual(actual_results, expected_results)

    def test_10_find_object_file_group_id(self):
        """ test_10_find_object_file_group_id """
        item = {'objectFileGroupId': '123', 'filePath': '456'}
        actual_results = _find_object_file_group_id(item)
        expected_results = '123'
        self.assertEqual(actual_results, expected_results)

    def test_11__find_default_file_path(self):
        """ test_11_find_default_image_id """
        item = {'filePath': 'some/path/123.jpg', 'fileId': 'google_file_id_456'}
        actual_results = _find_default_file_path(item)
        expected_results = 'some/path/123.jpg'
        self.assertEqual(actual_results, expected_results)

    def test_12_find_default_file_path(self):
        """ test_12_find_default_file_path """
        item = {'fileId': 'google_file_id_456'}
        actual_results = _find_default_file_path(item)
        expected_results = 'google_file_id_456'
        self.assertEqual(actual_results, expected_results)

    def test_13__add_sequence(self):
        """ test_13_add_sequence """
        standard_json = {
            'id': '123',
            'items': [{'id': '234'}, {"id": '345'}]
        }
        actual_results = _add_sequence(standard_json)
        expected_results = {
            'id': '123',
            'sequence': 0,
            'items': [{'id': '234', 'sequence': 1}, {"id": '345', 'sequence': 2}]
        }
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
