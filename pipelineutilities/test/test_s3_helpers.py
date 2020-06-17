import _set_path  # noqa
import unittest
from unittest.mock import patch
from unittest.mock import MagicMock
import boto3
from botocore.stub import Stubber
from pipelineutilities.s3_helpers import s3_file_exists, write_s3_file, write_s3_xml, filedata_is_already_on_s3, md5_checksum
import datetime
from dateutil.tz import tzutc


class TestS3Helpers(unittest.TestCase):

    @patch('pipelineutilities.s3_helpers.s3_client')
    def test_s3_file_exists_false_when_no_key(self, mock_s3_client):
        s3 = boto3.client("s3")
        stubber = Stubber(s3)
        stubber.add_client_error('head_object', service_message="message", expected_params={'Bucket': 'bucketnot', 'Key': 'key_not'})

        mock_s3_client.return_value = s3

        with stubber:
            response = s3_file_exists("bucketnot", "key_not")

        self.assertFalse(response)

    @patch('pipelineutilities.s3_helpers.s3_client')
    def test_s3_file_exists_returns_head_object_when_exists(self, mock_s3_client):
        s3 = boto3.client("s3")
        stubber = Stubber(s3)

        head_response = {'ResponseMetadata': {'RequestId': '2CA0C8ABC59ED601', 'HostId': 'W81yYPFfh/26bdCJGImLxHYIKQxKIABbu6uLSF8XhuDoPL3gtRsP9x39VyePZeP/XE4C8LHrp6Q=', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': 'W81yYPFfh/26bdCJGImLxHYIKQxKIABbu6uLSF8XhuDoPL3gtRsP9x39VyePZeP/XE4C8LHrp6Q=', 'x-amz-request-id': '2CA0C8ABC59ED601', 'date': 'Wed, 17 Jun 2020 13:28:49 GMT', 'last-modified': 'Wed, 17 Jun 2020 06:08:27 GMT', 'etag': '"f8663bf4e6705bdc617418b690b3e56c"', 'accept-ranges': 'bytes', 'content-type': 'text/json', 'content-length': '50451', 'server': 'AmazonS3'}, 'RetryAttempts': 0}, 'AcceptRanges': 'bytes', 'LastModified': datetime.datetime(2020, 6, 17, 6, 8, 27, tzinfo=tzutc()), 'ContentLength': 50451, 'ETag': '"f8663bf4e6705bdc617418b690b3e56c"', 'ContentType': 'text/json', 'Metadata': {}}  # noqa

        mock_s3_client.return_value = s3
        stubber.add_response('head_object', expected_params={'Bucket': 'bucket', 'Key': 'key'}, service_response=head_response)

        with stubber:
            response = s3_file_exists("bucket", "key")

        self.assertEqual(response, head_response)

    @patch('pipelineutilities.s3_helpers.s3_resource')
    @patch('pipelineutilities.s3_helpers.filedata_is_already_on_s3')
    def test_write_s3_file_writes_new_data_to_s3(self, mock_filedata_is_already_on_s3, mock_s3_resource):
        ""
        # s3 = boto3.resource("s3")
        # stubber = Stubber(s3)

        # mock_filedata_is_already_on_s3.return_value = s3
        # stubber.add_response('Object', expected_params={'Bucket': 'bucket', 'Key': 'key'}, service_response={"eTag": "etag"})

        # write_s3_file("bucket", "key", "data")
        # print(mock_s3_resource.Object("bucket", "key"))
        # print(mock_s3_resource.Object().put.called)

    @patch('pipelineutilities.s3_helpers.write_s3_file')
    def test_write_s3_xml(self, mock_write_s3_file):
        mock_write_s3_file.return_value = None

        write_s3_xml("bucket", "key", "xml")

        mock_write_s3_file.assert_called_once_with("bucket", "key", "xml", ContentType='application/xml')
        ""

    @patch('pipelineutilities.s3_helpers.s3_file_exists')
    @patch('pipelineutilities.s3_helpers.md5_checksum')
    @patch('pipelineutilities.s3_helpers.s3_client')
    def test_filedata_is_already_on_s3_false_if_the_file_does_not_exist(self, mock_s3_client, mock_md5_checksum, mock_s3_file_exists):
        # it is always false no matter if the etag matches.
        mock_s3_file_exists.return_value = False
        self.assertFalse(filedata_is_already_on_s3('bucket', 'key', 'data'))

    @patch('pipelineutilities.s3_helpers.s3_file_exists')
    @patch('pipelineutilities.s3_helpers.md5_checksum')
    @patch('pipelineutilities.s3_helpers.s3_client')
    def test_filedata_is_already_on_s3_true_if_the_etags_match(self, mock_s3_client, mock_md5_checksum, mock_s3_file_exists):
        # it is true if the etags match
        mock_s3_file_exists.return_value = {'ETag': '"etag"'}
        mock_md5_checksum.return_value = 'etag'

        self.assertTrue(filedata_is_already_on_s3('bucket', 'key', 'data'))

    @patch('pipelineutilities.s3_helpers.s3_file_exists')
    @patch('pipelineutilities.s3_helpers.md5_checksum')
    @patch('pipelineutilities.s3_helpers.s3_client')
    def test_filedata_is_already_on_s3_false_if_the_etags_do_not_match(self, mock_s3_client, mock_md5_checksum, mock_s3_file_exists):
        # it is false if the etags don't match
        mock_s3_file_exists.return_value = {'ETag': '"etag"'}
        mock_md5_checksum.return_value = 'new_etag'

        self.assertFalse(filedata_is_already_on_s3('bucket', 'key', 'data'))

    def test_md5_checksum_does_hex_md5(self):
        result = "098f6bcd4621d373cade4e832627b4f6"
        self.assertEqual(result, md5_checksum("test"))
