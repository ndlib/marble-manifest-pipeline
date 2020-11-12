# test_add_image_records_as_child_items.py
""" test add_image_records_as_child_items """
import unittest
import os
from additional_functions import get_seed_nodes_json, return_None_if_needed, \
    get_value_from_labels, remove_nodes_from_dictionary, exclude_if_pattern_matches, strip_unwanted_whitespace


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        self.image_info = {
            "1990_005_001-v0003.jpg": {
                "id": "1MrC-QDlkdMDR0rgdTENqwdlD7lY3AGe6",
                "name": "1990_005_001-v0003.jpg",
                "mimeType": "image/jpeg",
                "parents": [
                    "1SqZenJGXRtDrPcNOAzE9aR8dE9k9esMj"
                ],
                "modifiedTime": "2019-07-01T15:42:29.933Z",
                "driveId": "0AGid-WpPHOiEUk9PVA",
                "md5Checksum": "2ae1d9f082fc0aad7ca7086bdad20ef0",
                "size": "5526047"
            },
            "1990_005_001-v0004.jpg": {
                "id": "1lxLLreSLq3v1bbvO9FsD3PMf4IV-m238",
                "name": "1990_005_001-v0004.jpg",
                "mimeType": "image/jpeg",
                "parents": [
                    "1SqZenJGXRtDrPcNOAzE9aR8dE9k9esMj"
                ],
                "modifiedTime": "2019-07-01T15:42:41.648Z",
                "driveId": "0AGid-WpPHOiEUk9PVA",
                "md5Checksum": "65a7bc5841defa6e7f9bce0f89c12411",
                "size": "5651708"
            }
        }

    def test_02_get_seed_nodes_json(self):
        """ test_02 _get_seed_nodes_json """
        json_node = {
            "id": "id1", "collectionId": "col1", "sourceSystem": "system1", "repository": "repo1",
            "app_version": "appV1", "fileCreatedDate": "today"
        }
        seed_nodes_control = [
            {"collectionId": "collectionId"}, {"parentId": "id"}, {"sourceSystem": "sourceSystem"}, {"repository": "repository"},
            {"apiVersion": "apiVersion"}, {"fileCreatedDate": "fileCreatedDate"}
        ]
        seeded_nodes = get_seed_nodes_json(json_node, seed_nodes_control)
        expected_nodes = {'collectionId': 'col1', 'parentId': 'id1', 'sourceSystem': 'system1', 'repository': 'repo1', 'fileCreatedDate': 'today'}
        self.assertEqual(seeded_nodes, expected_nodes)

    def test_03_return_None_if_needed(self):
        """ test_03 return_None_if_needed """
        field = {"optional": True}
        value_passed = {}
        returned_value = return_None_if_needed(value_passed, field)
        expected_value = None
        self.assertEqual(returned_value, expected_value)
        field = {"optional": False}
        returned_value = return_None_if_needed(value_passed, field)
        expected_value = {}
        self.assertEqual(returned_value, expected_value)
        value_passed = {"other": "values"}
        returned_value = return_None_if_needed(value_passed, field)
        expected_value = {"other": "values"}
        self.assertEqual(returned_value, expected_value)

    def test_04_get_value_from_labels(self):
        """ test_04 get_value_from_labels """
        field = {"fromLabels": ["one", "two"]}
        value_passed = {"one": 1, "two": 2, "three": 3}
        returned_value = get_value_from_labels(value_passed, field)
        expected_value = 1
        self.assertEqual(returned_value, expected_value)
        field = {"fromLabels": ["two"]}
        returned_value = get_value_from_labels(value_passed, field)
        expected_value = 2
        self.assertEqual(returned_value, expected_value)

    def test_05_remove_nodes_from_dictionary(self):
        """ test_05 remove_nodes_from_dictionary """
        field = {"removeNodes": ["one", "two"]}
        value_passed = {"one": 1, "two": 2, "three": 3}
        remove_nodes_from_dictionary(value_passed, field)
        expected_value = {"three": 3}
        self.assertEqual(value_passed, expected_value)

    def test_06_exclude_if_pattern_matches(self):
        """ test_06 exclude_if_pattern_matches """
        exclude_pattern = [".shtml", "xyz"]
        value_passed = "filename.shtml"
        returned_value = exclude_if_pattern_matches(exclude_pattern, value_passed)
        expected_value = None
        self.assertEqual(returned_value, expected_value)
        value_passed = "filename.pdf"
        returned_value = exclude_if_pattern_matches(exclude_pattern, value_passed)
        expected_value = "filename.pdf"
        self.assertEqual(returned_value, expected_value)
        value_passed = "filexyzname.pdf"
        returned_value = exclude_if_pattern_matches(exclude_pattern, value_passed)
        expected_value = None
        self.assertEqual(returned_value, expected_value)

    def test_07_strip_unwanted_whitespace(self):
        """ test_07 strip_unwanted_whitespace """
        value_passed = " test string "
        returned_value = strip_unwanted_whitespace(value_passed)
        expected_value = "test string"
        self.assertEqual(returned_value, expected_value)

    def test_08_file_name_from_filePath(self):
        """ test_08 file_name_from_filePath """
        value_passed = "some/long/path/plus/filename.pdf"
        returned_value = os.path.basename(value_passed)
        expected_value = "filename.pdf"
        self.assertEqual(returned_value, expected_value)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
