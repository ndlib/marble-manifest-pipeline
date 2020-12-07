""" test save_json_to_dynamo """
import _set_path  # noqa
import unittest
from unittest.mock import patch
from pipelineutilities.save_standard_json_to_dynamo import SaveStandardJsonToDynamo


class Test(unittest.TestCase):
    """ Class for test fixtures """

    def test_01_get_related_ids_s3_key(self):
        """ test_01_read_related_ids """
        config = {"local": True, "process-bucket": "some_bucket_name", "pipeline-control-folder": "pipeline_control", "related-ids-file": "related_ids.json"}
        save_standard_json_to_dynamo_class = SaveStandardJsonToDynamo(config)
        actual_results = save_standard_json_to_dynamo_class._get_related_ids_s3_key()
        expected_results = "pipeline_control/related_ids.json"
        self.assertEqual(actual_results, expected_results)

    def test_02_read_related_ids(self):
        """ test_02_read_related_ids """
        config = {"local": True, "process-bucket": "some_bucket_name", "pipeline-control-folder": "pipeline_control", "related-ids-file": "related_ids.json"}
        save_standard_json_to_dynamo_class = SaveStandardJsonToDynamo(config)
        self.assertFalse(save_standard_json_to_dynamo_class.related_ids_updated)
        self.assertEqual(save_standard_json_to_dynamo_class.related_ids, {})

    def test_03_append_related_ids(self):
        """ test_03_append_related_ids """
        config = {"local": True, "process-bucket": "some_bucket_name", "pipeline-control-folder": "pipeline_control", "related-ids-file": "related_ids.json"}
        save_standard_json_to_dynamo_class = SaveStandardJsonToDynamo(config)
        standard_json = {'id': 'parent', 'childIds': [{"id": "id1", "sequence": 1}, {"id": "id2", "sequence": 2}]}
        actual_results = save_standard_json_to_dynamo_class._append_related_ids(standard_json)
        expected_results = {'id1': {'parentId': 'parent', 'sequence': 1}, 'id2': {'parentId': 'parent', 'sequence': 2}}
        self.assertEqual(actual_results, expected_results)
        standard_json = {'id': 'another_parent', 'childIds': [{"id": "id3", "sequence": 1}, {"id": "id4", "sequence": 2}]}
        actual_results = save_standard_json_to_dynamo_class._append_related_ids(standard_json)
        expected_results = {
            'id1': {'parentId': 'parent', 'sequence': 1},
            'id2': {'parentId': 'parent', 'sequence': 2},
            'id3': {'parentId': 'another_parent', 'sequence': 1},
            'id4': {'parentId': 'another_parent', 'sequence': 2}
        }
        self.assertEqual(actual_results, expected_results)

    @patch('pipelineutilities.save_standard_json_to_dynamo.SaveStandardJsonToDynamo._read_related_ids')
    def test_04_mock_read_related_ids(self, mock_read_related_ids):
        """ test_04_mock_read_related_ids """
        config = {"local": True, "process-bucket": "some_bucket_name", "pipeline-control-folder": "pipeline_control", "related-ids-file": "related_ids.json"}
        mock_read_related_ids.return_value = {'id1': {'parentId': 'new_parent', 'sequence': 1}, 'id2': {'parentId': 'parent', 'sequence': 2}}
        save_standard_json_to_dynamo_class = SaveStandardJsonToDynamo(config)
        actual_results = save_standard_json_to_dynamo_class.related_ids
        expected_results = {'id1': {'parentId': 'new_parent', 'sequence': 1}, 'id2': {'parentId': 'parent', 'sequence': 2}}
        self.assertEqual(actual_results, expected_results)

    @patch('pipelineutilities.save_standard_json_to_dynamo.SaveStandardJsonToDynamo._read_related_ids')
    def test_05_optionally_update_parent_id(self, mock_read_related_ids):
        """ test_05_optionally_update_parent_id """
        config = {"local": True, "process-bucket": "some_bucket_name", "pipeline-control-folder": "pipeline_control", "related-ids-file": "related_ids.json"}
        mock_read_related_ids.return_value = {'id1': {'parentId': 'new_parent', 'sequence': 1}, 'id2': {'parentId': 'parent', 'sequence': 2}}
        save_standard_json_to_dynamo_class = SaveStandardJsonToDynamo(config)
        standard_json = {"id": "id1", "parentId": "old_parent"}
        actual_results = save_standard_json_to_dynamo_class._optionally_update_parent_id(standard_json)
        expected_results = {"id": "id1", "parentId": "new_parent", "sequence": 1}
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    # save_data_for_tests()
    suite()
    unittest.main()
