""" handler """

import boto3
from boto3.dynamodb.conditions import Key  # , Attr
from botocore.exceptions import ClientError
import os
from pathlib import Path
import sentry_sdk   # noqa: E402
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration


if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(dsn=os.environ['SENTRY_DSN'], integrations=[AwsLambdaIntegration()])


def run(event: dict, context: dict) -> dict:
    event['fileRecordsDeleted'] = delete_file_records(event.get('website-metadata-tablename'), event.get('deleteAllFileRecords', False))  # 2252 .xml files deleted
    event['fileToHarvestRecordsDeleted'] = delete_file_to_process_records(event.get('website-metadata-tablename'), event.get('deleteAllFileToProcessRecords', False))  # 5488 records deleted
    delete_specific_file_records(event.get('website-metadata-tablename'))
    return event


def delete_file_records(table_name: str, delete_all_records: bool):
    """ Delete certain File records, depending on criteria below """
    print("deleting File records")
    pk = 'FILE'
    sk = 'FILE#'
    kwargs = {}
    kwargs['KeyConditionExpression'] = Key('PK').eq(pk) & Key('SK').begins_with(sk)
    kwargs['ProjectionExpression'] = 'PK, SK, sourceType'
    done = False
    records_deleted = 0
    table = boto3.resource('dynamodb').Table(table_name)
    while not done:
        results = query_dynamo_records(table_name, **kwargs)
        with table.batch_writer() as batch:
            for record in results.get('Items', []):
                delete_record_flag = False
                file_extension = Path(record.get('SK')).suffix
                if file_extension and '.' in file_extension and file_extension.lower() in ['.xml', '.jpg']:  # '.jpg'
                    delete_record_flag = True
                elif record.get('sourceType') == 'Google':
                    delete_record_flag = True
                elif file_extension and file_extension.lower() in ['.jpg'] and record.get('sourceType') in ['Museum', 'Curate']:
                    delete_record_flag = True
                if delete_record_flag or delete_all_records:
                    print('deleting record = ', record.get('SK'))
                    # delete_dynamo_record(table_name, record.get('PK'), record.get('SK'))
                    batch.delete_item(Key={'PK': record.get('PK'), 'SK': record.get('SK')})
                    records_deleted += 1
        if results.get('LastEvaluatedKey'):
            kwargs['ExclusiveStartKey'] = results.get('LastEvaluatedKey')
        else:
            done = True
    return records_deleted


def delete_specific_file_records(table_name: str):
    files_to_delete = [
        'digital/MARBLE-images/MAN/MAN_002204607/MAN_002204607_001.tif',
        'digital/MARBLE-images/MAN/MAN_002203932/MAN_002203932_001.tif',
        'digital/MARBLE-images/MAN/MAN_002204679/MAN_002204679_001.tif',
        'digital/MARBLE-images/MAN/MAN_002203917/MAN_002203917_001.tif',
        'digital/MARBLE-images/MAN/MAN_002204270/MAN_002204270_001.tif',
        'digital/MARBLE-images/MAN/MAN_002204454/MAN_002204454_001.tif',
        'digital/MARBLE-images/MAN/MAN_002203090/MAN_002203090_002.tif',
        'digital/MARBLE-images/MAN/MAN_002203090/MAN_002203090_001.tif',
        'digital/MARBLE-images/MAN/MAN_002204223/MAN_002204223.tif',
        'digital/MARBLE-images/MAN/MAN_002204679/MAN_002204679_002.tif',
        'digital/MARBLE-images/MAN/MAN_002204488/MAN_002204488_001.tif',
        'digital/MARBLE-images/MAN/MAN_002204607/MAN_002204607_002.tif',
        'digital/MARBLE-images/MAN/MAN_002204603/MAN_002204603_001.tif',
        'digital/MARBLE-images/MAN/MAN_002203518/MAN_002203518_001.tif',
        'digital/MARBLE-images/MAN/MAN_002202558/MAN_002202558_001.tif',
        'digital/MARBLE-images/MAN/MAN_002204763/MAN_002204763_002.tif',
        'digital/MARBLE-images/MAN/MAN_002204366/MAN_002204366_001.tif',
        'digital/MARBLE-images/MAN/MAN_002203265/MAN_002203265_001.tif',
        'digital/MARBLE-images/MAN/MAN_002203292/MAN_002203292_001.tif',
        'digital/MARBLE-images/MAN/MAN_002203894/MAN_002203894_001.tif',
        'digital/MARBLE-images/MAN/MAN_002204291/MAN_002204291_001.tif',
        'digital/MARBLE-images/MAN/MAN_002202427/MAN_002202427_001.tif',
        'digital/MARBLE-images/MAN/MAN_002203941/MAN_002203941_001.tif',
        'digital/MARBLE-images/MAN/MAN_002204510/MAN_002204510_001.tif',
        'digital/MARBLE-images/MAN/MAN_002203306/MAN_002203306_001.tif',
        'digital/MARBLE-images/MAN/MAN_002203043/MAN_002203043_001.tif',
        'digital/MARBLE-images/MAN/MAN_002203079/MAN_002203079_001.tif',
        'digital/MARBLE-images/MAN/MAN_002204714/MAN_002204714_001.tif',
        'digital/MARBLE-images/MAN/MAN_002204310/MAN_002204310_001.tif',
        'digital/MARBLE-images/MAN/MAN_002204104/MAN_002204104_001.tif',
        'digital/MARBLE-images/MAN/MAN_002203079/MAN_002203079_002.tif',
        'digital/MARBLE-images/MAN/MAN_002204763/MAN_002204763_001.tif',
        'digital/MARBLE-images/MAN/MAN_002203518/MAN_002203518_002.tif',
        'digital/MARBLE-images/MAN/MAN_002202548/MAN_002202548_001.tif',
        'digital/MARBLE-images/MAN/MAN_002204554/MAN_002204554_001.tif',
        'digital/MARBLE-images/MAN/MAN_002203943/MAN_002203943_001.tif',
        'digital/MARBLE-images/MAN/MAN_002204373/MAN_002204373_001.tif',
        'digital/MARBLE-images/MAN/MAN_002203084/MAN_002203084_001.tif',
        'digital/MARBLE-images/MAN/MAN_002201785/MAN_002201785_001.tif',
        'digital/MARBLE-images/MAN/MAN_002204371/MAN_002204371_001.tif',
        'digital/MARBLE-images/MAN/MAN_002203043/MAN_002203043_002.tif',
        'digital/MARBLE-images/MAN/MAN_002202558/MAN_002202558_002.tif',
        'digital/MARBLE-images/MAN/MAN_002203434/MAN_002203434_001.tif',
        'digital/MARBLE-images/MAN/MAN_002204685/MAN_002204685_002.tif',
        'digital/MARBLE-images/MAN/MAN_002204579/MAN_002204579_001.tif',
        'digital/MARBLE-images/MAN/MAN_002204790/MAN_002204790_001.tif',
        'digital/MARBLE-images/MAN/MAN_002204121/MAN_002204121_001.tif',
        'digital/MARBLE-images/MAN/MAN_002204554/MAN_002204554_002.tif',
        'digital/MARBLE-images/MAN/MAN_002204685/MAN_002204685_001.tif',
        'digital/MARBLE-images/MAN/MAN_002203935/MAN_002203935_001.tif'
    ]
    table = boto3.resource('dynamodb').Table(table_name)
    with table.batch_writer() as batch:
        for file_name in files_to_delete:
            PK = 'FILE'
            SK = 'FILE#' + file_name.upper()
            batch.delete_item(Key={'PK': PK, 'SK': SK})
            batch.delete_item(Key={'PK': 'FILETOPROCESS', 'SK': 'FILEPATH#' + file_name.upper()})


def delete_file_to_process_records(table_name: str, delete_all_records: bool):
    """ Delete certain FileToProcess records, depending on criteria below """
    print("deleting FileToProcess records")
    pk = 'FILETOPROCESS'
    sk = 'FILEPATH#'
    kwargs = {}
    kwargs['KeyConditionExpression'] = Key('PK').eq(pk) & Key('SK').begins_with(sk)
    kwargs['ProjectionExpression'] = 'PK, SK, sourceType'
    done = False
    records_deleted = 0
    table = boto3.resource('dynamodb').Table(table_name)
    while not done:
        results = query_dynamo_records(table_name, **kwargs)
        with table.batch_writer() as batch:
            for record in results.get('Items', []):
                delete_record_flag = False
                file_extension = Path(record.get('SK')).suffix
                if file_extension and '.' in file_extension and file_extension.lower() in ['.xml', '.jpg']:  # '.jpg'
                    delete_record_flag = True
                elif record.get('sourceType') == 'Google':
                    delete_record_flag = True
                elif file_extension and file_extension.lower() in ['.jpg'] and record.get('sourceType') in ['Museum', 'Curate']:
                    delete_record_flag = True
                if delete_record_flag or delete_all_records:
                    print('deleting record = ', record.get('SK'))
                    records_deleted += 1
                    # delete_dynamo_record(table_name, record.get('PK'), record.get('SK'))
                    batch.delete_item(Key={'PK': record.get('PK'), 'SK': record.get('SK')})
        if results.get('LastEvaluatedKey'):
            kwargs['ExclusiveStartKey'] = results.get('LastEvaluatedKey')
        else:
            done = True
    return records_deleted


def query_dynamo_records(table_name: str, **kwargs) -> dict:
    """ very generic dynamo query """
    response = {}
    try:
        table = boto3.resource('dynamodb').Table(table_name)
        response = table.query(**kwargs)
    except ClientError as ce:
        sentry_sdk.capture_exception(ce)
    return response


def delete_dynamo_record(table_name: str, pk: str, sk: str) -> dict:
    """ use this to delete a single record.  use batch delete to delete in huge batches """
    kwargs = {}
    kwargs['Key'] = {'PK': pk, 'SK': sk}
    try:
        table = boto3.resource('dynamodb').Table(table_name)
        response = table.delete_item(**kwargs)
    except ClientError as ce:
        sentry_sdk.capture_exception(ce)
    return response

# setup:
# export SSM_KEY_BASE=/all/stacks/steve-manifest
# aws-vault exec testlibnd-superAdmin
# python -c 'from handler import *; test()'

# testing:
# python 'run_all_tests.py'


def test(identifier=""):
    """ test exection """
    event = {}
    event['website-metadata-tablename'] = 'steve-manifest-websiteMetadata470E321C-1NPIJYOXUCVHU'
    event['website-metadata-tablename'] = 'marbleb-prod-manifest-websiteMetadata470E321C-5EJSG31E16Z7'
    # event['website-metadata-tablename'] = 'marbleb-test-manifest-websiteMetadata470E321C-JJG277N1OMMC'
    event['deleteAllFileRecords'] = False
    event['deleteAllFileToProcessRecords'] = False
    # print(1 / 0)  # make sure we don't accidentally run this
    event = run(event, {})
    print(event)
