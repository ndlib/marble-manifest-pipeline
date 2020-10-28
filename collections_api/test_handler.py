# test_handler.py
""" test handler """
import _set_path  # noqa
import unittest
import os
# from pathlib import Path
# from dependencies.pipelineutilities.pipeline_config import setup_pipeline_config
from handler import _find_right_dict_in_list, _get_appropriate_event_dict, _update_original_event


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """

    def test_01_find_right_dict_in_list(self):
        """ test_01_find_right_dict_in_list """
        event = [{'something': True}, {'but_not_collections': True}]
        key_to_find = 'collectionsApiComplete'
        actual_results = _find_right_dict_in_list(event, key_to_find)
        expected_results = None
        self.assertEqual(actual_results, expected_results)

    def test_02_find_right_dict_in_list(self):
        """ test_02_find_right_dict_in_list """
        key_to_find = 'collectionsApiComplete'
        event = [{'something': True}, {'but_not_collections': True}, {key_to_find: False}]
        actual_results = _find_right_dict_in_list(event, key_to_find)
        expected_results = 2
        self.assertEqual(actual_results, expected_results)

    def test_03_get_appropriate_event_dict(self):
        """ test_03_get_appropriate_event_dict """
        event = {'dict': True}
        actual_results = _get_appropriate_event_dict(event)
        expected_results = {'dict': True}
        self.assertEqual(actual_results, expected_results)

    def test_04_get_appropriate_event_dict(self):
        """ test_04_get_appropriate_event_dict """
        event = [{'dict': True}]
        actual_results = _get_appropriate_event_dict(event)
        expected_results = {'collectionsApiComplete': False, 'local': False}
        expected_event = [{'dict': True}, {'collectionsApiComplete': False, 'local': False}]
        self.assertEqual(actual_results, expected_results)
        self.assertEqual(2, len(event))
        self.assertTrue(event, expected_event)

    def test_05_get_appropriate_event_dict(self):
        """ test_05_get_appropriate_event_dict """
        event = [{'dict': True}, {'collectionsApiComplete': False, 'local': False}]
        actual_results = _get_appropriate_event_dict(event)
        expected_results = {'collectionsApiComplete': False, 'local': False}
        expected_event = [{'dict': True}, {'collectionsApiComplete': False, 'local': False}]
        self.assertEqual(actual_results, expected_results)
        self.assertEqual(2, len(event))
        self.assertTrue(event, expected_event)

    def test_06_update_original_event(self):
        """ test_06_update_original_event """
        event = [{'dict': True}, {'collectionsApiComplete': False, 'local': False}]
        myevent = {'something': 'here', 'collectionsApiComplete': True}
        actual_results = _update_original_event(event, myevent)
        expected_results = [{'dict': True}, {'something': 'here', 'collectionsApiComplete': True, 'local': False}]
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    # save_data_for_tests()
    suite()
    unittest.main()
