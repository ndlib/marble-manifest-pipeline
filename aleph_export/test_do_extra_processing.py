# test_transform_marc_json.py
""" test transform_marc_json """
import _set_path  # noqa
import unittest
import os
from do_extra_processing import do_extra_processing, _lookup_work_type, _format_subjects, \
    _format_creators, _translate_repository, _format_call_number


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """

    def test_1_lookup_work_type(self):
        """ test _lookup_work_type"""
        result = _lookup_work_type("a")
        self.assertTrue(result == "Language material")

    def test_2_format_subjects(self):
        subjects_list = ["test", "test_with^^^example_uri"]
        actual_output = _format_subjects(subjects_list)
        expected_output = [{'term': 'test'}, {'term': 'test_with', 'uri': 'example_uri'}]
        self.assertTrue(actual_output == expected_output)

    def test_3_format_creators(self):
        creators_list = ["test_creator", "test_creator_with^^^life_dates"]
        actual_output = _format_creators(creators_list)
        expected_output = [
            {'attribution': '', 'fullName': 'test_creator', 'display': 'test_creator'},
            {'attribution': '', 'fullName': 'test_creator_with', 'lifeDates': 'life_dates', 'display': 'test_creator_with'}
        ]
        self.assertTrue(actual_output == expected_output)

    def test_4_link_to_source(self):
        actual_output = do_extra_processing("abc123", "link_to_source")
        expected_output = "https://onesearch.library.nd.edu/permalink/f/1phik6l/ndu_alephabc123"
        self.assertTrue(actual_output == expected_output)

    def test_5_translate_repository(self):
        actual_output = _translate_repository("rare")
        expected_output = "RARE"
        self.assertTrue(actual_output == expected_output)
        actual_output = _translate_repository("hesb")
        expected_output = "hesb"
        self.assertTrue(actual_output == expected_output)
        actual_output = _translate_repository("something_else")
        expected_output = "something_else"
        self.assertTrue(actual_output == expected_output)

    def test_6_format_call_number(self):
        call_number_list = "M 1744 .B868^^^G4 1796"
        actual_output = _format_call_number(call_number_list)
        expected_output = "M 1744 .B868 G4 1796"
        self.assertTrue(actual_output == expected_output)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
