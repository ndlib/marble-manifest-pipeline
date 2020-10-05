""" test_standard_json_helpers """
import _set_path  # noqa: F401
import os
from pipelineutilities.get_size_of_images import get_size_of_images, _get_image_size_given_uri
import unittest
from unittest.mock import patch


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Test report missing fields """

    @patch('pipelineutilities.get_size_of_images._get_json_given_url')
    def test_01_get_image_size_given_uri(self, mock_get_json_given_url):
        """ test_01_get_image_size_given_uri """
        uri = ' https://image-iiif.library.nd.edu/iiif/2/1976.046%2F1976_046-v0002'
        mock_get_json_given_url.return_value = {
            "@context": "http://iiif.io/api/image/2/context.json",
            "@id": "https://image-iiif.library.nd.edu/iiif/2/1976.046%2F1976_046-v0002",
            "protocol": "http://iiif.io/api/image",
            "width": 1,
            "height": 2,
        }
        actual_results = _get_image_size_given_uri(uri)
        expected_results = {'height': 2, 'width': 1}
        self.assertEqual(actual_results, expected_results)

    @patch('pipelineutilities.get_size_of_images._get_json_given_url')
    def test_02_get_size_of_images(self, mock_get_json_given_url):
        """ test_01_get_image_size_given_uri """
        standard_json = {
            'id': 'abc',
            'items': [{
                'id': '123',
                'level': 'file',
                'iiifImageUri': 'https://image-iiif.library.nd.edu/iiif/2/1976.046%2F1976_046-v0002'
            }]
        }
        mock_get_json_given_url.return_value = {
            "@context": "http://iiif.io/api/image/2/context.json",
            "@id": "https://image-iiif.library.nd.edu/iiif/2/1976.046%2F1976_046-v0002",
            "protocol": "http://iiif.io/api/image",
            "width": 1,
            "height": 2,
        }
        actual_results = get_size_of_images(standard_json)
        expected_results = {
            'id': 'abc',
            'items': [{
                'id': '123',
                'level': 'file',
                'iiifImageUri': 'https://image-iiif.library.nd.edu/iiif/2/1976.046%2F1976_046-v0002',
                "width": 1,
                "height": 2,
            }]
        }
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
