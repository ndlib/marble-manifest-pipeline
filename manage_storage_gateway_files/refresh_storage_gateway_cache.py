""" Refresh Storage Gateway Cache """

import boto3


def _get_file_share_arn_given_s3_arn(s3_bucket_name: str) -> str:
    """ Find ARN for Storage Gateway File Share given S3 bucket name """
    file_share_arn = ""
    client = boto3.client('storagegateway')
    for file_share in client.list_file_shares()['FileShareInfoList']:
        if file_share['FileShareType'] == 'SMB':
            s3_arn_found = _get_s3_arn_given_smb_file_share(file_share['FileShareARN'])
            print("s3_arn_found = ", s3_arn_found)
            if s3_bucket_name in s3_arn_found:
                file_share_arn = file_share['FileShareARN']
                print("s3_arn_found = ", s3_arn_found, 'for s3 bucket =', s3_bucket_name, 'FileShareARN =', file_share_arn)
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
    file_share_arn = _get_file_share_arn_given_s3_arn(s3_bucket_name)
    client = boto3.client('storagegateway')
    response = client.refresh_cache(FileShareARN=file_share_arn, FolderList=['/'], Recursive=True)
    if response.get('ResponseMetadata').get('HTTPStatusCode') == 200:
        return True
    return False


def test():
    # s3_bucket_name = 'testlibnd-smb-test'  # testlibnd
    s3_bucket_name = 'libnd-smb-marble'  # libnd
    print(refreshStorageGatewayCache(s3_bucket_name))

# establish a connection using aws-vault then execute the following command
# python -c "import refresh_storage_gateway_cache; refresh_storage_gateway_cache.test()"
