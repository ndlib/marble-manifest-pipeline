import _set_path  # noqa
import unittest
import os
from unittest.mock import patch
import pipelineutilities.s3_sync


class TestS3Sync(unittest.TestCase):

    def test_01_get_source_files_list(self):
        """ test _get_source_files_list """
        local_folder = os.path.dirname(os.path.realpath(__file__))
        files_list = pipelineutilities.s3_sync._get_source_files_list(local_folder)
        self.assertTrue(os.path.basename(__file__) in files_list)

    @patch('pipelineutilities.s3_sync._get_source_files_list')
    def test_02_patch_get_source_files_list(self, mock_get_source_files_list):
        local_folder = "/this/will/be/mocked"
        files_list_result = ['somedir/file.name']
        mock_get_source_files_list.return_value = files_list_result
        files_list = pipelineutilities.s3_sync._get_source_files_list(local_folder)
        mock_get_source_files_list.assert_called_once_with(local_folder)
        self.assertTrue(files_list == files_list_result)

    @patch('pipelineutilities.s3_sync._get_source_files_list')
    def test_04_s3_sync(self, mock_get_source_files_list):
        files_list_result = ['somedir/file.name']
        mock_get_source_files_list.return_value = files_list_result
        s3_bucket = "marble-manifest-prod-processbucket-13bond538rnnb"
        s3_root_key = "sites"
        local_folder_path = "/this/will/be/mocked"
        results = pipelineutilities.s3_sync.s3_sync(s3_bucket, s3_root_key, local_folder_path)
        mock_get_source_files_list.assert_called_once_with(local_folder_path)
        self.assertFalse(results)

    @patch('pipelineutilities.s3_sync._get_source_files_list')
    def test_05_s3_sync(self, mock_get_source_files_list):
        files_list_result = [os.path.basename(__file__)]
        mock_get_source_files_list.return_value = files_list_result
        s3_bucket = "fake_bucket_name"
        s3_root_key = "sites"
        local_folder_path = os.path.dirname(os.path.realpath(__file__))
        results = pipelineutilities.s3_sync.s3_sync(s3_bucket, s3_root_key, local_folder_path)
        mock_get_source_files_list.assert_called_once_with(local_folder_path)
        self.assertFalse(results)


if __name__ == '__main__':
    unittest.main()
