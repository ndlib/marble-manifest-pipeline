""" test_standard_json_helpers """
import _set_path  # noqa: F401
import json
import os
from pipelineutilities.standard_json_helpers import _remove_brackets, _remove_trailing_punctuation, _clean_up_standard_json_strings, \
    _load_language_codes, _add_language_display, _clean_up_standard_json_recursive, _add_publishers_node
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
            "title": "test0",
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
            "title": "test0,",
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


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
