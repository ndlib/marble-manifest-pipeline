# test_do_extra_processing.py
""" test do_extra_processing """
import _set_path  # noqa
import unittest
import os
from datetime import date
from do_extra_processing import _format_creators_given_string, _format_creators, _format_subjects, \
    _format_publisher, get_seed_nodes_json, do_extra_processing


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        self.event = {"local": True}

    def test_01_format_creators_given_string(self):
        contributor = "Sorin, Edward"
        actual_results = _format_creators_given_string(contributor)
        expected_results = {'fullName': 'Sorin, Edward', 'display': 'Sorin, Edward'}
        self.assertTrue(actual_results == expected_results)

    def test_02_format_creators(self):
        contributor = ["Sorin, Edward"]
        actual_results = _format_creators(contributor)
        expected_results = [{'fullName': 'Sorin, Edward', 'display': 'Sorin, Edward'}]
        # print("actual_results = ", actual_results)
        self.assertTrue(actual_results == expected_results)
        contributor = [['Cavanaugh, John, 1870-1935', 'Ayscough, John, 1858-1928 (substituted for  Walsh, David I. (David Ignatius), 1872-1947']]
        actual_results = _format_creators(contributor)
        expected_results = [
            {'fullName': 'Cavanaugh, John, 1870-1935', 'display': 'Cavanaugh, John, 1870-1935'},
            {'fullName': 'Ayscough, John, 1858-1928 (substituted for  Walsh, David I. (David Ignatius), 1872-1947', 'display': 'Ayscough, John, 1858-1928 (substituted for  Walsh, David I. (David Ignatius), 1872-1947'}  # noqa: E501
        ]
        # print("actual_results = ", actual_results)
        self.assertTrue(actual_results == expected_results)

    def test_03_format_subjects(self):
        value_passed = "some_subject"
        actual_results = _format_subjects(value_passed)
        # print("actual_results = ", actual_results)
        expected_results = [{'term': 'some_subject'}]
        self.assertTrue(actual_results == expected_results)

    def test_04_format_publisher(self):
        value_passed = "some_publisher_name"
        actual_results = _format_publisher(value_passed)
        # print("actual_results = ", actual_results)
        expected_results = {'publisherName': 'some_publisher_name'}
        self.assertTrue(actual_results == expected_results)

    def test_05_get_seed_nodes_json(self):
        """ test_5 get_seed_nodes_json """
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
        self.assertTrue(seeded_nodes == expected_nodes)

    def test_06_do_extra_processing_link_to_source(self):
        value_passed = "https://curate.nd.edu/api/items/zp38w953h0s"
        extra_processing = "link_to_source"
        actual_results = do_extra_processing(value_passed, extra_processing, {}, "", "", {})
        # print("actual_results = ", actual_results)
        expected_results = "https://curate.nd.edu/show/zp38w953h0s"
        self.assertTrue(actual_results == expected_results)

    def test_07_do_extra_processing_schema_api_version(self):
        value_passed = ""
        extra_processing = "schema_api_version"
        actual_results = do_extra_processing(value_passed, extra_processing, {}, "", 1, {})
        # print("actual_results = ", actual_results)
        expected_results = 1
        self.assertTrue(actual_results == expected_results)

    def test_08_do_extra_processing_file_created_date(self):
        value_passed = ""
        extra_processing = "file_created_date"
        actual_results = do_extra_processing(value_passed, extra_processing, {}, "", 1, {})
        # print("actual_results = ", actual_results)
        expected_results = str(date.today())
        self.assertTrue(actual_results == expected_results)

    def test_09_get_seed_nodes_json(self):
        """ test_9 get_seed_nodes_json """
        json_node = {
            "containedFiles": [
                {
                    "id": "pz50gt57r3h",
                    "fileUrl": "https://curate.nd.edu/api/items/pz50gt57r3h",
                    "downloadUrl": "https://curate.nd.edu/api/items/download/pz50gt57r3h",
                    "thumbnailUrl": "https://curate.nd.edu/downloads/pz50gt57r3h/thumbnail",
                    "bendoItem": "pv63fx74g23",
                    "dateSubmitted": "2018-12-10Z",
                    "modified": "2020-01-29Z",
                    "title": "University of Notre Dame Commencement Program",
                    "label": "1845-08-02_Commencement.pdf",
                    "mimeType": "application/pdf",
                    "access": {"readGroup": ["public"], "editPerson": ["skirycki"]},
                    "depositor": "batch_ingest",
                    "owner": "skirycki",
                    "hasModel": "GenericFile",
                    "isPartOf": ["https://curate.nd.edu/api/items/pv63fx74g23"]
                }
            ]
        }
        seed_nodes_control = [{"containedFiles": "containedFiles"}]
        actual_results = get_seed_nodes_json(json_node, seed_nodes_control)
        # print("actual_results = ", actual_results)
        expected_results = json_node
        self.assertTrue(actual_results == expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
