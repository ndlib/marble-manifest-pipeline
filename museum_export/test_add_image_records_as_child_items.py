# test_add_image_records_as_child_items.py
""" test add_image_records_as_child_items """
import unittest
from add_image_records_as_child_items import AddImageRecordsAsChildItems


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

    def test_01_add_child_content_from_parent(self):
        """ test_01 _add_child_content_from_parent """
        sample_object = {"id": "abc", "collectionId": "col1", "parentId": "mom", "sourceSystem": "authoritative system", "repository": "sample repository"}
        process_one_museum_object_class = AddImageRecordsAsChildItems({})
        actual_results = process_one_museum_object_class._add_child_content_from_parent(sample_object)
        expected_results = {'collectionId': 'col1', 'parentId': 'abc', 'sourceSystem': 'authoritative system', 'repository': 'sample repository', 'objectFileGroupId': 'abc'}
        self.assertEqual(actual_results, expected_results)

    def test_02_format_image_as_child(self):
        """ test_02 _format_image_as_child """
        digital_asset = {
            "sequence": 1,
            "thumbnail": True,
            "fileDescription": "1990_005_001-v0004.jpg",
            "filePath": "/Media/images/1990/1990/1990_005_001/1990_005_001-v0004.jpg"
        }
        process_one_museum_object_class = AddImageRecordsAsChildItems(self.image_info)
        image_item = process_one_museum_object_class._create_item_record_for_image(digital_asset)
        expected_object = {
            "id": "1990_005_001-v0004.jpg",
            "thumbnail": True,
            "level": "file",
            "description": "1990_005_001-v0004.jpg",
            "md5Checksum": "65a7bc5841defa6e7f9bce0f89c12411",
            "filePath": "https://drive.google.com/a/nd.edu/file/d/1lxLLreSLq3v1bbvO9FsD3PMf4IV-m238/view",
            "sequence": 1,
            "size": 5651708,
            "title": "1990_005_001-v0004.jpg",
            "fileId": "1lxLLreSLq3v1bbvO9FsD3PMf4IV-m238",
            "modifiedDate": "2019-07-01T15:42:41.648Z",
            "mimeType": "image/jpeg"
        }
        self.assertEqual(image_item, expected_object)

    def test_03_add_images_as_children(self):
        """ Test_03 _add_images_as_children """
        object = {
            "id": "1990.005.001", "collectionId": "1990.005.001", "parentId": "root", "sourceSystem": "EmbARK", "repository": "museum",
            "digitalAssets": [
                {
                    "sequence": 1,
                    "thumbnail": True,
                    "fileDescription": "1990_005_001-v0004.jpg",
                    "filePath": "/Media/images/1990/1990/1990_005_001/1990_005_001-v0004.jpg"
                },
                {
                    "sequence": 2,
                    "thumbnail": False,
                    "fileDescription": "1990_005_001-v0003.jpg",
                    "filePath": "/Media/images/1990/1990/1990_005_001/1990_005_001-v0003.jpg"
                }
            ]
        }
        process_one_museum_object_class = AddImageRecordsAsChildItems(self.image_info)
        actual_results = process_one_museum_object_class.add_images_as_children(object)
        # print("actual_results = ", actual_results)
        expected_results = {
            'id': '1990.005.001',
            'collectionId': '1990.005.001',
            'parentId': 'root',
            'sourceSystem': 'EmbARK',
            'repository': 'museum',
            'items': [
                {
                    'id': '1990_005_001-v0004.jpg',
                    'level': 'file',
                    'sequence': 1,
                    'title': '1990_005_001-v0004.jpg',
                    'description': '1990_005_001-v0004.jpg',
                    'thumbnail': True,
                    'md5Checksum': '65a7bc5841defa6e7f9bce0f89c12411',
                    'filePath': 'https://drive.google.com/a/nd.edu/file/d/1lxLLreSLq3v1bbvO9FsD3PMf4IV-m238/view',
                    'fileId': '1lxLLreSLq3v1bbvO9FsD3PMf4IV-m238',
                    'modifiedDate': '2019-07-01T15:42:41.648Z',
                    'mimeType': 'image/jpeg',
                    'size': 5651708,
                    'collectionId': '1990.005.001',
                    'parentId': '1990.005.001',
                    'sourceSystem': 'EmbARK',
                    'repository': 'museum',
                    'objectFileGroupId': '1990.005.001'
                },
                {
                    'id': '1990_005_001-v0003.jpg',
                    'level': 'file',
                    'sequence': 2,
                    'title': '1990_005_001-v0003.jpg',
                    'description': '1990_005_001-v0003.jpg',
                    'thumbnail': False,
                    'md5Checksum': '2ae1d9f082fc0aad7ca7086bdad20ef0',
                    'filePath': 'https://drive.google.com/a/nd.edu/file/d/1MrC-QDlkdMDR0rgdTENqwdlD7lY3AGe6/view',
                    'fileId': '1MrC-QDlkdMDR0rgdTENqwdlD7lY3AGe6',
                    'modifiedDate': '2019-07-01T15:42:29.933Z',
                    'mimeType': 'image/jpeg',
                    'size': 5526047,
                    'collectionId': '1990.005.001',
                    'parentId': '1990.005.001',
                    'sourceSystem': 'EmbARK',
                    'repository': 'museum',
                    'objectFileGroupId': '1990.005.001'
                }
            ]
        }
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
