""" test save_default_file_metadata_to_dynamo """
import _set_path  # noqa
import unittest
from unittest.mock import patch
from pipelineutilities.save_default_file_metadata_to_dynamo import SaveDefaultFileMetadataToDynamo


class Test(unittest.TestCase):
    """ Class for test fixtures """

    def test_01_find_default_file_node(self):
        """ test_01_find_default_file_node """
        config = {"local": True, "process-bucket": "some_bucket_name", "pipeline-control-folder": "pipeline_control", "related-ids-file": "related_ids.json"}
        standard_json = {
            "id": "abc", "defaultFilePath": "file_path_for_file_234", "copyrightStatement": "Some meaningful copyright statement", "copyrightStatus": "some copyright status",
            "items": [
                {"id": "123", "level": "file", "objectGroupFileId": "wrong one"},
                {"id": "234", "level": "file", "thumbnail": True, "objectFileGroupId": "right one"}
            ]
        }
        save_default_file_metadata_to_dynamo_class = SaveDefaultFileMetadataToDynamo(config)
        actual_results = save_default_file_metadata_to_dynamo_class._find_default_file_node(standard_json)
        expected_results = {"id": "234", "level": "file", "thumbnail": True, "objectFileGroupId": "right one"}
        self.assertEqual(actual_results, expected_results)

    def test_02_create_new_node_from_standard_json(self):
        """ test_02_create_new_node_from_standard_json """
        config = {"local": True, "process-bucket": "some_bucket_name", "pipeline-control-folder": "pipeline_control", "related-ids-file": "related_ids.json"}
        standard_json = {
            "id": "abc", "defaultFilePath": "file_path_for_file_234", "copyrightStatement": "Some meaningful copyright statement", "copyrightStatus": "some copyright status",
            "items": [
                {"id": "123", "level": "file", "objectGroupFileId": "wrong one"},
                {"id": "234", "level": "file", "thumbnail": True, "objectFileGroupId": "right one"}
            ]
        }
        save_default_file_metadata_to_dynamo_class = SaveDefaultFileMetadataToDynamo(config)
        default_file_node = save_default_file_metadata_to_dynamo_class._find_default_file_node(standard_json)
        actual_results = save_default_file_metadata_to_dynamo_class._create_new_node_from_standard_json(standard_json, default_file_node)
        expected_results = {"id": "abc", "defaultFilePath": "file_path_for_file_234", "copyrightStatement": "Some meaningful copyright statement", "copyrightStatus": "some copyright status", "objectFileGroupId": "right one"}
        self.assertEqual(actual_results, expected_results)

    def test_03_merge_data_to_save(self):
        """ test_03_merge_data_to_save """
        config = {"local": True, "process-bucket": "some_bucket_name", "pipeline-control-folder": "pipeline_control", "related-ids-file": "related_ids.json"}
        existing_dynamo_record = {}
        new_node_from_standard_json = {"id": "abc", "defaultFilePath": "file_path_for_file_234",
                                       "copyrightStatement": "Some meaningful copyright statement", "copyrightStatus": "some copyright status", "objectFileGroupId": "right one"}
        save_default_file_metadata_to_dynamo_class = SaveDefaultFileMetadataToDynamo(config)
        actual_results = save_default_file_metadata_to_dynamo_class._merge_data_to_save(existing_dynamo_record, new_node_from_standard_json)
        expected_results = {"id": "abc", "defaultFilePath": "file_path_for_file_234", "copyrightStatement": "Some meaningful copyright statement", "copyrightStatus": "some copyright status", "objectFileGroupId": "right one"}
        self.assertEqual(actual_results, expected_results)

    def test_04_merge_data_to_save(self):
        """ test_04_merge_data_to_save """
        config = {"local": True, "process-bucket": "some_bucket_name", "pipeline-control-folder": "pipeline_control", "related-ids-file": "related_ids.json",
                  "default-file-metadata-tablename": "some_table_name"}
        existing_dynamo_record = {"id": "abc", "copyrightStatement": "You can't use this", "copyrightStatus": "uber copyrighted", "objectFileGroupId": "new one"}
        new_node_from_standard_json = {"id": "abc", "defaultFilePath": "file_path_for_file_234",
                                       "copyrightStatement": "Some meaningful copyright statement", "copyrightStatus": "some copyright status", "objectFileGroupId": "right one"}
        save_default_file_metadata_to_dynamo_class = SaveDefaultFileMetadataToDynamo(config)
        actual_results = save_default_file_metadata_to_dynamo_class._merge_data_to_save(existing_dynamo_record, new_node_from_standard_json)
        expected_results = {"id": "abc", "defaultFilePath": "file_path_for_file_234", "copyrightStatement": "You can't use this", "copyrightStatus": "uber copyrighted", "objectFileGroupId": "new one"}
        self.assertEqual(actual_results, expected_results)

    @patch('pipelineutilities.save_default_file_metadata_to_dynamo.SaveDefaultFileMetadataToDynamo._get_existing_dynamo_record')
    def test_05_mock_get_existing_dynamo_record(self, mock_get_existing_dynamo_record):
        """ test_05_mock_get_existing_dynamo_record """
        config = {"local": True, "process-bucket": "some_bucket_name", "pipeline-control-folder": "pipeline_control", "related-ids-file": "related_ids.json",
                  "default-file-metadata-tablename": "some_table_name"}
        mock_get_existing_dynamo_record.return_value = {"id": "abc", "copyrightStatement": "You can't use this", "copyrightStatus": "uber copyrighted", "objectFileGroupId": "new one"}
        save_default_file_metadata_to_dynamo_class = SaveDefaultFileMetadataToDynamo(config)
        actual_results = save_default_file_metadata_to_dynamo_class._get_existing_dynamo_record("some id")
        expected_results = {"id": "abc", "copyrightStatement": "You can't use this", "copyrightStatus": "uber copyrighted", "objectFileGroupId": "new one"}
        self.assertEqual(actual_results, expected_results)

    @patch('pipelineutilities.save_default_file_metadata_to_dynamo.SaveDefaultFileMetadataToDynamo._save_json_to_dynamo')
    @patch('pipelineutilities.save_default_file_metadata_to_dynamo.SaveDefaultFileMetadataToDynamo._get_existing_dynamo_record')
    def test_06_save_default_file_metadata(self, mock_get_existing_dynamo_record, mock_save_json_to_dynamo):
        """ test_06_save_default_file_metadata """
        config = {"local": True, "default-file-metadata-tablename": "some_table_name"}
        mock_get_existing_dynamo_record.return_value = {"id": "abc", "copyrightStatement": "You can't use this", "copyrightStatus": "uber copyrighted", "objectFileGroupId": "new one"}
        mock_save_json_to_dynamo.return_value = True
        save_default_file_metadata_to_dynamo_class = SaveDefaultFileMetadataToDynamo(config)
        standard_json = {
            "id": "abc", "defaultFilePath": "file_path_for_file_234", "copyrightStatement": "Some meaningful copyright statement", "copyrightStatus": "some copyright status",
            "items": [
                {"id": "123", "level": "file", "objectGroupFileId": "wrong one"},
                {"id": "234", "level": "file", "thumbnail": True, "objectFileGroupId": "right one"}
            ]
        }
        actual_results = save_default_file_metadata_to_dynamo_class.save_default_file_metadata(standard_json)
        expected_results = {"id": "abc", "defaultFilePath": "file_path_for_file_234", "copyrightStatement": "You can't use this", "copyrightStatus": "uber copyrighted", "objectFileGroupId": "new one"}
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    # save_data_for_tests()
    suite()
    unittest.main()
