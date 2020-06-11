# test_clean_up_standard_json.py
""" test clean_up_standard_json """
import _set_path  # noqa
import unittest
# import json
import os
from pathlib import Path
from clean_up_standard_json import _fix_level, _fix_ids
from pipelineutilities.pipeline_config import setup_pipeline_config  # noqa: E402


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        self.event = {"local": True}
        self.event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
        self.config = setup_pipeline_config(self.event)

    def test_01_fix_level(self):
        standard_json = {
            "id": "id1", "level": "manifest",
            "items": [
                {"id": "id2", "level": "file"},
                {"id": "id3", "level": "manifest",
                    "items": [{"id": "id3.1", "level": "file"}, {"id": "id3.2", "level": "file"}]},
            ]
        }
        actual_results = _fix_level(standard_json)
        expected_results = {
            'id': 'id1', 'level': 'collection',
            'items': [
                {'id': 'id2', 'level': 'file'},
                {'id': 'id3', 'level': 'manifest',
                    'items': [
                        {'id': 'id3.1', 'level': 'file'},
                        {'id': 'id3.2', 'level': 'file'}]}]}
        # print("actual_results = ", actual_results)
        self.assertTrue(actual_results == expected_results)

    def test_02_fix_ids(self):
        standard_json = {
            "id": "id1", "parentId": "root",
            "items": [
                {"id": "id2", "collectionId": "c_id2", "parentId": "p_id2"},
                {"id": "id3", "collectionId": "c_id3", "parentId": "p_id3",
                    "items": [{"id": "id3.1"}, {"id": "id3.2"}]},
            ]
        }
        actual_results = _fix_ids(standard_json)
        expected_results = {
            'id': 'id1', 'collectionId': 'id1', 'parentId': 'root',
            'items': [
                {'id': 'id2', 'collectionId': 'id1', 'parentId': 'id1'},
                {'id': 'id3', 'collectionId': 'id1', 'parentId': 'id1',
                    'items': [
                        {'id': 'id3.1', 'collectionId': 'id1', 'parentId': 'id3'},
                        {'id': 'id3.2', 'collectionId': 'id1', 'parentId': 'id3'}]}]}
        # print("actual_results = ", actual_results)
        self.assertTrue(actual_results == expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
