import _set_test_path  # noqa
import unittest
from pathlib import Path
from pipeline_config import setup_pipeline_config
import datetime
from directory_api.handler import success, error, convert_directory_to_json, convert_object_to_json, load_from_s3_or_cache

base_config = {}
base_config['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
base_config['local'] = True
base_config['test'] = True
config = setup_pipeline_config(base_config)

directories = load_from_s3_or_cache(config)


class TestDirecgtoryApi(unittest.TestCase):

    def test_success_method(self):
        test_response = {
            "statusCode": 200,
            "body": '"hi"',
            "headers": {
                "Access-Control-Allow-Origin": "*",
            },
        }
        self.assertEqual(success("hi"), test_response)

    def test_success_method_converts_datatime_objects(self):
        now = datetime.datetime.now()
        test_response = {
            "statusCode": 200,
            "body": '{"datetime": "' + now.isoformat() + '", "array": ["1", "2", "3"]}',
            "headers": {
                "Access-Control-Allow-Origin": "*",
            },
        }
        test_data = {
            "datetime": now,
            "array": [
                "1", "2", "3"
            ]
        }
        self.assertEqual(success(test_data), test_response)

    def test_error_response(self):
        test_response = {
            "statusCode": 404,
            "headers": {
                "Access-Control-Allow-Origin": "*",
            },
        }
        self.assertEqual(error(404), test_response)

    def test_convert_directory_to_json_for_multiple_objects(self):
        # test for multiple objects
        directory = 'collections-ead_xml-images-BPP_1001'

        test = {'id': 'collections-ead_xml-images-BPP_1001', 'uri': 'https://presentation-iiif.library.nd.edu/directories/collections-ead_xml-images-BPP_1001', 'path': 'collections/ead_xml/images/BPP_1001', 'mode': 'multi-volume', 'numberOfFiles': 472}
        response = convert_directory_to_json(directories[directory])

        self.assertEqual('multi-volume', test['mode'])
        self.assertEqual(472, test['numberOfFiles'])
        self.assertEqual(response, test)

        # test traverse
        response = convert_directory_to_json(directories[directory], True)
        self.assertEqual(327, len(response.get('objects', [])))

    def test_convert_directory_to_json_for_single_objects(self):
        # test for single objects
        directory = 'digital-MARBLE-images-BOO_000297305'

        test = {'id': 'digital-MARBLE-images-BOO_000297305', 'uri': 'https://presentation-iiif.library.nd.edu/directories/digital-MARBLE-images-BOO_000297305', 'path': 'digital/MARBLE-images/BOO_000297305', 'mode': 'single-volume', 'numberOfFiles': 50}
        response = convert_directory_to_json(directories[directory])

        self.assertEqual('single-volume', test['mode'])
        self.assertEqual(50, test['numberOfFiles'])
        self.assertEqual(response, test)

        response = convert_directory_to_json(directories[directory], True)
        self.assertEqual(1, len(response.get('objects', [])))

    def test_convert_object_to_json(self):
        directory = 'collections-ead_xml-images-BPP_1001'
        item = 'collections-ead_xml-images-BPP_1001-BPP_1001-001'

        response = convert_object_to_json(directories[directory]['objects'][item])

        test = {'lastModified': '2018-12-11T15:56:06+00:00', 'source': 'RBSC', 'directory_id': 'collections-ead_xml-images-BPP_1001', 'files': [{'eTag': '"7a13120979586f01c26eee078c201539-1"', 'fileId': 'collections-ead_xml-images-BPP_1001-BPP_1001-001', 'key': 'collections/ead_xml/images/BPP_1001/BPP_1001-001-F2.jpg', 'label': 'https://rarebooks library nd edu/collections/ead xml/images/BPP 1001/BPP 1001 001 F2', 'lastModified': '2018-12-11T15:54:35+00:00', 'path': 's3://libnd-smb-rbsc/collections/ead_xml/images/BPP_1001/BPP_1001-001-F2.jpg', 'size': 2360533, 'source': 'RBSC', 'sourceUri': 'https://rarebooks.library.nd.edu/collections/ead_xml/images/BPP_1001/BPP_1001-001-F2.jpg', 'storageClass': 'STANDARD', 'iiifImageFilePath': 's3://marble-data-broker-publicbucket-1kvqtwnvkhra2/collections/ead_xml/images/BPP_1001/BPP_1001-001-F2.jpg', 'iiifImageUri': 'https://image-iiif.library.nd.edu/iiif/2/collections/ead_xml/images/BPP_1001/BPP_1001-001-F2.jpg'}, {'eTag': '"d5bb2f86fdd431527897267252825807-1"', 'fileId': 'collections-ead_xml-images-BPP_1001-BPP_1001-001', 'key': 'collections/ead_xml/images/BPP_1001/BPP_1001-001.jpg', 'label': 'https://rarebooks library nd edu/collections/ead xml/images/BPP 1001/BPP 1001 001', 'lastModified': '2018-12-11T15:56:06+00:00', 'path': 's3://libnd-smb-rbsc/collections/ead_xml/images/BPP_1001/BPP_1001-001.jpg', 'size': 2165484, 'source': 'RBSC', 'sourceUri': 'https://rarebooks.library.nd.edu/collections/ead_xml/images/BPP_1001/BPP_1001-001.jpg', 'storageClass': 'STANDARD', 'iiifImageFilePath': 's3://marble-data-broker-publicbucket-1kvqtwnvkhra2/collections/ead_xml/images/BPP_1001/BPP_1001-001.jpg', 'iiifImageUri': 'https://image-iiif.library.nd.edu/iiif/2/collections/ead_xml/images/BPP_1001/BPP_1001-001.jpg'}], 'id': 'collections-ead_xml-images-BPP_1001-BPP_1001-001', 'path': 'collections/ead_xml/images/BPP_1001', 'uri': 'https://presentation-iiif.library.nd.edu/directories/collections-ead_xml-images-BPP_1001/objects/collections-ead_xml-images-BPP_1001-BPP_1001-001'}
        self.assertEqual(response, test)
