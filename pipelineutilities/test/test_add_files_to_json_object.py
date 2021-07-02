# test_create_json_from_xml.py
""" test create_json_from_xml """
import _set_path  # noqa
import os
import json
import unittest
from pipelineutilities.add_files_to_json_object import change_file_extensions_to_tif, _fix_file_metadata_not_on_s3  # , AddFilesToJsonObject


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        pass

    def test_04_change_file_extensions_to_tif(self):
        """ test_04_change_file_extensions_to_tif """
        config = {}
        config['local'] = True
        each_file_dict = {
            "id": "1995.033.001/1995.033.001.a/1995_033_001_a-v0004.jpg",
            "title": "1995_033_001_a-v0004.jpg",
            "description": "1995_033_001_a-v0004.jpg",
            "mimeType": "image/jpeg",
            "filePath": "1995.033.001/1995.033.001.a/1995_033_001_a-v0004.jpg"
        }
        actual_results = change_file_extensions_to_tif(each_file_dict, ['.pdf'])
        expected_results = {
            'id': '1995.033.001/1995.033.001.a/1995_033_001_a-v0004.tif',
            'title': '1995_033_001_a-v0004.tif',
            'description': '1995_033_001_a-v0004.tif',
            'mimeType': 'image/jpeg',
            'filePath': '1995.033.001/1995.033.001.a/1995_033_001_a-v0004.tif'
        }
        self.assertEqual(actual_results, expected_results)

    def test_05_change_file_extensions_to_tif(self):
        """ test_05_change_file_extensions_to_tif """
        config = {}
        config['local'] = True
        each_file_dict = {
            "id": "https://rarebooks.library.nd.edu/digital/bookreader/MSN-EA_8011-1-B/images/MSN-EA_8011-01-B-000a.jpg"
        }
        actual_results = change_file_extensions_to_tif(each_file_dict, ['.pdf'])
        expected_results = {
            'id': 'https://rarebooks.library.nd.edu/digital/bookreader/MSN-EA_8011-1-B/images/MSN-EA_8011-01-B-000a.tif'
        }
        self.assertEqual(actual_results, expected_results)

    def test_06_change_file_extensions_to_tif(self):
        """ test_06_change_file_extensions_to_tif """
        config = {}
        config['local'] = True
        each_file_dict = {
            "id": "https://rarebooks library nd edu/digital/bookreader/MSN EA 8011 1 B/images/ B 000a"
        }
        actual_results = change_file_extensions_to_tif(each_file_dict, ['.pdf'])
        expected_results = {
            'id': 'https://rarebooks library nd edu/digital/bookreader/MSN EA 8011 1 B/images/ B 000a'
        }
        self.assertEqual(actual_results, expected_results)

    def test_07_fix_file_metadata_not_on_s3(self):
        """ test_07_fix_file_metadata_not_on_s3 """
        file_item = {
            "description": "Collegiate Jazz Festival 1960",
            "filePath": "http://archives.nd.edu/ndcjf/dcjf1960.jpg",
            "id": "dcjf1960.jpg",
            "treePath": "CJF_EAD/aspace_b55b4e723bdd5a5cee9f67f18e66fbb1/aspace_0d7c59e17cb4e513ffd55cabdd751059/aspace_82ca319eea9fa935d315cbf2bd491de0/"
        }
        parent_unique_identifier = 'abc'
        media_file_extensions = ['.pdf', '.mp3', '.mp4']
        actual_results = _fix_file_metadata_not_on_s3(file_item, parent_unique_identifier, media_file_extensions)

        expected_results = {
            'description': 'Collegiate Jazz Festival 1960',
            'filePath': 'CJF_EAD/aspace_b55b4e723bdd5a5cee9f67f18e66fbb1/aspace_0d7c59e17cb4e513ffd55cabdd751059/aspace_82ca319eea9fa935d315cbf2bd491de0/dcjf1960.jpg',
            'id': 'CJF_EAD/aspace_b55b4e723bdd5a5cee9f67f18e66fbb1/aspace_0d7c59e17cb4e513ffd55cabdd751059/aspace_82ca319eea9fa935d315cbf2bd491de0/dcjf1960.jpg',
            'treePath': 'CJF_EAD/aspace_b55b4e723bdd5a5cee9f67f18e66fbb1/aspace_0d7c59e17cb4e513ffd55cabdd751059/aspace_82ca319eea9fa935d315cbf2bd491de0/',
            'sourceUri': 'https://archives.nd.edu/ndcjf/dcjf1960.jpg',
            'storageSystem': 'Uri',
            'sourceType': 'Uri',
            'typeOfData': 'Uri',
            'title': 'dcjf1960.jpg',
            'objectFileGroupId': 'abc',
            'imageGroupId': 'abc'}
        self.assertEqual(actual_results, expected_results)

    def test_07_fix_file_metadata_not_on_s3_part2(self):
        """ test_07_fix_file_metadata_not_on_s3_part2 """
        file_item = {
            "description": "Collegiate Jazz Festival 1960",
            "filePath": "http://archives.nd.edu/ndcjf/dcjf1960.pdf",
            "id": "dcjf1960.pdf",
            "treePath": "CJF_EAD/aspace_b55b4e723bdd5a5cee9f67f18e66fbb1/aspace_0d7c59e17cb4e513ffd55cabdd751059/aspace_82ca319eea9fa935d315cbf2bd491de0/"
        }
        parent_unique_identifier = 'abc'
        media_file_extensions = ['.pdf', '.mp3', '.mp4']
        actual_results = _fix_file_metadata_not_on_s3(file_item, parent_unique_identifier, media_file_extensions)
        expected_results = {
            'description': 'Collegiate Jazz Festival 1960',
            'filePath': 'CJF_EAD/aspace_b55b4e723bdd5a5cee9f67f18e66fbb1/aspace_0d7c59e17cb4e513ffd55cabdd751059/aspace_82ca319eea9fa935d315cbf2bd491de0/dcjf1960.pdf',
            'id': 'CJF_EAD/aspace_b55b4e723bdd5a5cee9f67f18e66fbb1/aspace_0d7c59e17cb4e513ffd55cabdd751059/aspace_82ca319eea9fa935d315cbf2bd491de0/dcjf1960.pdf',
            "treePath": "CJF_EAD/aspace_b55b4e723bdd5a5cee9f67f18e66fbb1/aspace_0d7c59e17cb4e513ffd55cabdd751059/aspace_82ca319eea9fa935d315cbf2bd491de0/",
            'sourceUri': 'https://archives.nd.edu/ndcjf/dcjf1960.pdf',
            'mediaGroupId': 'abc',
            'title': 'dcjf1960.pdf',
            "storageSystem": "Uri",
            "sourceType": "Uri",
            "typeOfData": "Uri"
        }
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
