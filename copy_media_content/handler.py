# handler.py
""" Module to launch application """

import _set_path  # noqa
import os
from pathlib import Path
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
import urllib.parse
import boto3
from botocore.exceptions import ClientError

if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(dsn=os.environ['SENTRY_DSN'], integrations=[AwsLambdaIntegration()])


def run(event, _context):
    """ Run the process to retrieve and process Aleph metadata. """
    print("event =", event)
    # Get the object from the event and show its content type
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print("key =", key)
    if 'public-access/' in key:  # We will only trigger a move for content not in the public-access folder
        print('Skipping because file is in public-access folder.')
        return
    if 'temp/' in key:  # We will only trigger a move for content not in the public-access folder
        print('Skipping because file is in temp folder.')
        return
    if '/._' in key:
        print("Skipping hidden resource fork files.")
        return
    file_extension = Path(key).suffix
    print("file_extension =", file_extension)
    if not file_extension or file_extension.lower() not in ['.mp3', '.mp4', '.wav', '.pdf']:  # We only want to move media files
        print("Skipping because file is not a media file.")
        return
    new_key = os.path.join('public-access/media', key)
    try:
        # s3 = boto3.client('s3')
        # response = s3.get_object(Bucket=bucket, Key=key)
        # mime_type = response.get('ContentType')  # keep for future reference
        source_location = {'Bucket': bucket_name, 'Key': key}
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        bucket.copy(source_location, new_key)
        print("file copied to ", new_key)
        refreshStorageGatewayCache(os.environ.get('FILE_SHARE_ARN', ''))
    except ClientError as e:
        print(e)
        sentry_sdk.capture_exception(e)


def refreshStorageGatewayCache(file_share_arn: str) -> bool:
    """ Refresh the cache on the Storage Gateway """
    if not file_share_arn:
        return False
    print("file_share_arn =", file_share_arn)
    client = boto3.client('storagegateway')
    response = client.refresh_cache(FileShareARN=file_share_arn, FolderList=['/'], Recursive=True)
    if response.get('ResponseMetadata').get('HTTPStatusCode') == 200:
        print("refreshed Storage Gateway cache for ", file_share_arn)
        return True
    return False


# setup:
# testlibnd -  export FILE_SHARE_ARN=arn:aws:storagegateway:us-east-1:333680067100:share/share-3CDC3057
# aws-vault exec testlibnd-superAdmin
# python -c 'from handler import *; test()'
def test():
    """ test exection """
    event = {}
    event = {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-east-1', 'eventTime': '2020-06-25T18:42:38.099Z', 'eventName': 'ObjectCreated:Put', 'userIdentity': {'principalId': 'AWS:AROAI6CZ2KVAQG2B4HD34:rdought1'}, 'requestParameters': {'sourceIPAddress': '98.32.232.192'}, 'responseElements': {'x-amz-request-id': '0EEA4E5B5B77C96F', 'x-amz-id-2': 'HoiHuQMD7tJraZ+l1XIRd/tmq74gB2thtXC/3r8x6vEPNOw7xyMMnIHqZFpzlRhnv+TilKahUH4Q98zG4p7La0grRPD7DXgT'}, 's3': {'s3SchemaVersion': '1.0', 'configurationId': 'NjQ2Y2M1YTAtMzFhNi00NjRjLTk3NWUtM2E5YjQ2MTNiZTZk', 'bucket': {'name': 'testlibnd-smb-test', 'ownerIdentity': {'principalId': 'A1LSOVJBH3XNJN'}, 'arn': 'arn:aws:s3:::devred-sample'}, 'object': {'key': 'Aleph/BOO_000297305/BOO_000297305_000001.tif', 'size': 87, 'eTag': 'd860c4f5d9e70b1d361be804f66b791c', 'sequencer': '005EF4F01FAAC12B20'}}}]}  # noqa: #501
    run(event, {})
    # refreshStorageGatewayCache(os.environ.get('FILE_SHARE_ARN', ''))
