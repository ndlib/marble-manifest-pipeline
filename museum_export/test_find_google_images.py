# test_find_google_images.py
""" test find_google_images """
import _set_path  # noqa
import unittest
from find_google_images import FindGoogleImages


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        self.find_google_images_class = FindGoogleImages({}, {})

    def test_01_build_google_query_string(self):
        """ test_01 _build_google_query_string """
        image_file_list = ["abc.jpg", "123.jpg"]
        google_query_string = self.find_google_images_class._build_google_query_string(image_file_list)
        expected_query_string = "trashed = False and mimeType contains 'image' and ( name = 'abc.jpg' or  name = '123.jpg')"
        self.assertEqual(google_query_string, expected_query_string)

    def test_02_save_file_info_to_hash(self):
        """ test_02 save_file_info_to_hash """
        query_results = [
            {
                "id": "1k3X8fGfrBQyQaJtj4CGmg9C0nOt5XauA",
                "name": "1951_004_014-v0002.jpg",
                "mimeType": "image/jpeg",
                "parents": [
                    "1G6i8EtB_SYauPKHLm-nJhLV3jk3q4p0Q"
                ],
                "modifiedTime": "2020-04-02T17:00:25.302Z",
                "driveId": "0AGid-WpPHOiEUk9PVA",
                "md5Checksum": "bfbe007b8111d515a06905389d757c8a",
                "size": "4772856"
            },
            {
                "id": "1JHUNou4Z1izI6C-X2Tw2ZgUKN8eAOJAY",
                "name": "1947_001-v0001.jpg",
                "mimeType": "image/jpeg",
                "parents": [
                    "1A-fhOXBzMSvCRq4aHO1AnNs6hyus1w6w"
                ],
                "modifiedTime": "2020-01-15T13:28:07.361Z",
                "driveId": "0AGid-WpPHOiEUk9PVA",
                "md5Checksum": "f4859e37938a805fc5ee5d8a17598912",
                "size": "566551"
            }
        ]
        result_hash = self.find_google_images_class._save_file_info_to_hash(query_results)
        expected_hash = {
            "1951_004_014-v0002.jpg": {
                "id": "1k3X8fGfrBQyQaJtj4CGmg9C0nOt5XauA",
                "name": "1951_004_014-v0002.jpg",
                "mimeType": "image/jpeg",
                "parents": [
                    "1G6i8EtB_SYauPKHLm-nJhLV3jk3q4p0Q"
                ],
                "modifiedTime": "2020-04-02T17:00:25.302Z",
                "driveId": "0AGid-WpPHOiEUk9PVA",
                "md5Checksum": "bfbe007b8111d515a06905389d757c8a",
                "size": "4772856"
            },
            "1947_001-v0001.jpg": {
                "id": "1JHUNou4Z1izI6C-X2Tw2ZgUKN8eAOJAY",
                "name": "1947_001-v0001.jpg",
                "mimeType": "image/jpeg",
                "parents": [
                    "1A-fhOXBzMSvCRq4aHO1AnNs6hyus1w6w"
                ],
                "modifiedTime": "2020-01-15T13:28:07.361Z",
                "driveId": "0AGid-WpPHOiEUk9PVA",
                "md5Checksum": "f4859e37938a805fc5ee5d8a17598912",
                "size": "566551"
            }
        }
        self.assertEqual(result_hash, expected_hash)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
