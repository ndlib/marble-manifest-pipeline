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

    # Get the object from the event and show its content type
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    if 'public-access/' in key:  # We will only trigger a move for content not in the public-access folder
        print('Skipping because file is in public-access folder.')
        return
    if 'temp/' in key:  # We will only trigger a move for content not in the public-access folder
        print('Skipping because file is in temp folder.')
        return
    file_extension = Path(key).suffix
    print("file_extension =", file_extension)
    if not file_extension or file_extension.lower() not in ['.mp3', '.mp4', '.wav', '.pdf':  # We only want to move media files
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
        refreshStorageGatewayCache(bucket_name)
    except ClientError as e:
        print(e)
        sentry_sdk.capture_exception(e)


def _get_file_share_arn_given_bucket_name(s3_bucket_name: str) -> str:
    """ Find ARN for Storage Gateway File Share given S3 bucket name """
    file_share_arn = ""
    client = boto3.client('storagegateway')
    for file_share in client.list_file_shares()['FileShareInfoList']:
        if file_share['FileShareType'] == 'SMB':
            s3_arn_found = _get_s3_arn_given_smb_file_share(file_share['FileShareARN'])
            if s3_bucket_name in s3_arn_found:
                file_share_arn = file_share['FileShareARN']
                # print("s3_arn_found = ", s3_arn_found, 'for s3 bucket =', s3_bucket_name, 'FileShareARN =', file_share_arn)
                break
    return file_share_arn


def _get_s3_arn_given_smb_file_share(smb_file_share_arn: str) -> str:
    """ Return S3 ARN associated with a named SMB file share ARN """
    s3_arn = ''
    client = boto3.client('storagegateway')
    for file_share in client.describe_smb_file_shares(FileShareARNList=[smb_file_share_arn])['SMBFileShareInfoList']:
        s3_arn = file_share['LocationARN']
    return s3_arn


def refreshStorageGatewayCache(s3_bucket_name: str) -> bool:
    """ Refresh the cache on the Storage Gateway """
    file_share_arn = _get_file_share_arn_given_bucket_name(s3_bucket_name)
    client = boto3.client('storagegateway')
    response = client.refresh_cache(FileShareARN=file_share_arn, FolderList=['/'], Recursive=True)
    if response.get('ResponseMetadata').get('HTTPStatusCode') == 200:
        print("refreshed Storage Gateway cache for ", s3_bucket_name)
        return True
    return False


# setup:
# export SSM_KEY_BASE=/all/stacks/steve-manifest
# aws-vault exec testlibnd-superAdmin
# python -c 'from handler import *; test()'
def test():
    """ test exection """
    event = {}
    event = {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-east-1', 'eventTime': '2020-06-25T18:42:38.099Z', 'eventName': 'ObjectCreated:Put', 'userIdentity': {'principalId': 'AWS:AROAI6CZ2KVAQG2B4HD34:rdought1'}, 'requestParameters': {'sourceIPAddress': '98.32.232.192'}, 'responseElements': {'x-amz-request-id': '0EEA4E5B5B77C96F', 'x-amz-id-2': 'HoiHuQMD7tJraZ+l1XIRd/tmq74gB2thtXC/3r8x6vEPNOw7xyMMnIHqZFpzlRhnv+TilKahUH4Q98zG4p7La0grRPD7DXgT'}, 's3': {'s3SchemaVersion': '1.0', 'configurationId': 'NjQ2Y2M1YTAtMzFhNi00NjRjLTk3NWUtM2E5YjQ2MTNiZTZk', 'bucket': {'name': 'testlibnd-smb-test', 'ownerIdentity': {'principalId': 'A1LSOVJBH3XNJN'}, 'arn': 'arn:aws:s3:::devred-sample'}, 'object': {'key': 'Aleph/BOO_000297305/BOO_000297305_000001.tif', 'size': 87, 'eTag': 'd860c4f5d9e70b1d361be804f66b791c', 'sequencer': '005EF4F01FAAC12B20'}}}]}
    run(event, {})
