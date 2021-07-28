""" handler """

import _set_path  # noqa
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import sentry_sdk   # noqa: E402
from dynamo_query_functions import scan_dynamo_records
import json
import os

s3 = boto3.resource('s3')


def query_dynamo_records(table_name: str, **kwargs) -> dict:
    """ very generic dynamo query """
    response = {}
    table = boto3.resource('dynamodb').Table(table_name)
    response = table.query(**kwargs)
    return response
    try:
        table = boto3.resource('dynamodb').Table(table_name)
        response = table.query(**kwargs)
    except ClientError as ce:
        sentry_sdk.capture_exception(ce)
    return response


def capture_all_unprocessed_records(table_name: str) -> list:
    unprocessed_records = []
    kwargs = {}
    kwargs['IndexName'] = 'GSI2'
    kwargs['KeyConditionExpression'] = Key('GSI2PK').eq("FILETOPROCESS") & Key('GSI2SK').eq("DATELASTPROCESSED#")
    while True:
        results = query_dynamo_records(table_name, **kwargs)
        for record in results.get('Items', []):
            unprocessed_records.append(record)
        if results.get('LastEvaluatedKey'):
            kwargs['ExclusiveStartKey'] = results.get('LastEvaluatedKey')
        else:
            break
    return unprocessed_records


def capture_item_record_essentials(table_name: str) -> dict:
    kwargs = {}
    kwargs['FilterExpression'] = Attr("PK").begins_with("ITEM#")
    kwargs['ProjectionExpression'] = 'PK, SK, #type, defaultFilePath'
    kwargs['ExpressionAttributeNames'] = {'#type': 'TYPE'}
    continued_count = 0
    files_referenced = {}
    while True:
        results = scan_dynamo_records(table_name, **kwargs)
        for record in results.get('Items', []):
            if record.get('defaultFilePath', '') and record.get('TYPE', '') in ['Item', 'SupplementalData']:
                if not record.get('defaultFilePath', '') in files_referenced:
                    files_referenced[record.get('defaultFilePath')] = []
                files_referenced[record.get('defaultFilePath')].append(record.copy())
        if results.get('LastEvaluatedKey'):
            kwargs['ExclusiveStartKey'] = results.get('LastEvaluatedKey')
            continued_count += 1
        else:
            break
    return files_referenced


def s3_file_exits(bucket_name: str, key: str) -> bool:
    if not bucket_name or not key:
        return False
    try:
        s3.Object(bucket_name, key).load()
    except ClientError as ce:  # noqa: F841
        return False
    return True


def save_initial_data(table_name: str, item_record_essentials_file_name: str, unprocessed_records_file_name: str):
    item_record_essentials = capture_item_record_essentials(table_name)
    with open(item_record_essentials_file_name, 'w') as output_file:
        json.dump(item_record_essentials, output_file, indent=2, default=str, sort_keys=True)

    unprocessed_records = capture_all_unprocessed_records(table_name)
    for each_record in unprocessed_records:
        if each_record.get('storageSystem') == 'S3':
            each_record['s3FileExists'] = s3_file_exits(each_record.get('sourceBucketName'), each_record.get('sourceFilePath'))
    with open(unprocessed_records_file_name, 'w') as output_file:
        json.dump(unprocessed_records, output_file, indent=2, default=str, sort_keys=True)


def clean_up_data(table_name, item_record_essentials_file_name: str, unprocessed_records_file_name: str):
    with open(item_record_essentials_file_name, 'r', encoding='utf-8') as json_file:
        item_record_essentials_dict = json.load(json_file)
    with open(unprocessed_records_file_name, 'r', encoding='utf-8') as json_file:
        unprocessed_records_list = json.load(json_file)
    i = 0
    print("cleaning_up_data in ", table_name)
    table = boto3.resource('dynamodb').Table(table_name)
    with table.batch_writer() as batch:
        for each_record in unprocessed_records_list:
            if each_record.get('storageSystem', '') == 'S3' and not each_record.get('s3FileExists', True):
                print("sourceFilePath =", each_record.get('sourceFilePath'))
                if each_record.get('sourceFilePath', '') in item_record_essentials_dict:
                    for record_to_update in item_record_essentials_dict[each_record.get('sourceFilePath')]:
                        print("update =", record_to_update)
                        remove_default_file_path_from_dynamo_records(table_name, record_to_update.get('PK'), record_to_update.get('SK'))
                        i += 1
                batch.delete_item(Key={'PK': 'FILETOPROCESS', 'SK': each_record.get('SK')})  # Delete known FileToProcess record
                batch.delete_item(Key={'PK': 'IMAGE', 'SK': each_record.get('SK').replace('FILEPATH', 'IMAGE')})  # Delete possible Image record
                # delete_dynamo_record(table_name, 'FILETOPROCESS', each_record.get('SK'))  # Delete known FileToProcess record
                # delete_dynamo_record(table_name, 'IMAGE', each_record.get('SK').replace('FILEPATH', 'IMAGE'))  # Delete possible Image record
    print("records updated", i)


def remove_default_file_path_from_dynamo_records(table_name: str, pk: str, sk: str):
    table = boto3.resource('dynamodb').Table(table_name)
    table.update_item(
        Key={'PK': pk, 'SK': sk},
        UpdateExpression='REMOVE defaultFilePath',
        ReturnValues="ALL_NEW"
    )


def delete_dynamo_record(table_name: str, pk: str, sk: str) -> dict:
    """ use this to delete a single record.  use batch delete to delete in huge batches """
    print("deleting ", pk, sk)
    kwargs = {}
    kwargs['Key'] = {'PK': pk, 'SK': sk}
    try:
        table = boto3.resource('dynamodb').Table(table_name)
        response = table.delete_item(**kwargs)
    except ClientError as ce:
        sentry_sdk.capture_exception(ce)
    return response


# setup:
# aws-vault exec testlibnd-superAdmin
# python -c 'from clean_up_FileToProcess_records import *; test()'


def test(identifier=""):
    """ When we moved content to a new bucket, we had many instances of FileToProcess records and Image records
        that pointed to a now-obsolete location.  This checks each FileToProcess record on S3, verifies if the file exists,
        and if not, deletes the FileToProcess record, any possible associated Image record, and updates
        Item and SupplementalData records that are associated by removing the defaultFilePath node.

        To use, first sign into aws-vault for testlibnd and run.  Verify results.  Then sign in to aws-vault for libnd and run. """

    testlibnd_tables = [
        'steve-manifest-websiteMetadata470E321C-1D6R3LX7EI284',
        'jon-test-manifest-websiteMetadata470E321C-ZCSU70JC12I0',
        'jon-prod-manifest-websiteMetadata470E321C-8NG755QB2S5I',
        'sm-test-manifest-websiteMetadata470E321C-E5FXU8HUIWQG',
        'sm-prod-manifest-websiteMetadata470E321C-HO7FZQXZXI8M',
        'testlib-prod-manifest-websiteMetadata470E321C-1XA9OOG7PJWEE',
        'testlib-test-manifest-websiteMetadata470E321C-1XQ2EFEWM3UXZ',
        'mlk-manifest-websiteMetadata470E321C-1M04DW0EXCC91'
    ]
    libnd_tables = [
        'marbleb-prod-manifest-websiteMetadata470E321C-5EJSG31E16Z7',
        'marbleb-test-manifest-websiteMetadata470E321C-JJG277N1OMMC'
    ]

    print("vault =", os.environ.get('AWS_VAULT'))
    if os.environ.get('AWS_VAULT') == 'testlibnd-superAdmin':
        tables_to_process = testlibnd_tables
        prefix = 'testlibnd_'
    if os.environ.get('AWS_VAULT') == 'libnd-power-user':
        tables_to_process = libnd_tables
        prefix = 'libnd_'

    for table_name in tables_to_process:
        print("processing table ", table_name)
        unprocessed_records_file_name = './clean_up_file_to_process_records/' + prefix + table_name + '_unprocessed_records.json'
        item_record_essentials_file_name = './clean_up_file_to_process_records/' + prefix + table_name + '_item_record_essentials.json'
        save_initial_data(table_name, item_record_essentials_file_name, unprocessed_records_file_name)
        clean_up_data(table_name, item_record_essentials_file_name, unprocessed_records_file_name)
