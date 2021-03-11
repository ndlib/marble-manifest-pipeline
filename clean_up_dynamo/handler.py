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
    event['fileRecordsDeleted'] = delete_file_records(event.get('website-metadata-tablename'))  # 2252 .xml files deleted
    event['fileToHarvestRecordsDeleted'] = delete_file_to_process_records(event.get('website-metadata-tablename'))  # 5488 records deleted
    return event


def delete_file_records(table_name: str):
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
                if file_extension and '.' in file_extension and file_extension.lower() in ['.xml']:  # '.jpg'
                    delete_record_flag = True
                elif record.get('sourceType') == 'Google':
                    delete_record_flag = True
                elif file_extension and file_extension.lower() in ['.jpg'] and record.get('sourceType') in ['Museum', 'Curate']:
                    delete_record_flag = True
                if delete_record_flag:
                    print('deleting record = ', record.get('SK'))
                    # delete_dynamo_record(table_name, record.get('PK'), record.get('SK'))
                    batch.delete_item(Key={'PK': record.get('PK'), 'SK': record.get('SK')})
                    records_deleted += 1
        if results.get('LastEvaluatedKey'):
            kwargs['ExclusiveStartKey'] = results.get('LastEvaluatedKey')
        else:
            done = True
    return records_deleted


def delete_file_to_process_records(table_name: str):
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
                if file_extension and '.' in file_extension and file_extension.lower() in ['.xml']:  # '.jpg'
                    delete_record_flag = True
                elif record.get('sourceType') == 'Google':
                    delete_record_flag = True
                elif file_extension and file_extension.lower() in ['.jpg'] and record.get('sourceType') in ['Museum', 'Curate']:
                    delete_record_flag = True
                if delete_record_flag:
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
# aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
# python -c 'from handler import *; test()'

# testing:
# python 'run_all_tests.py'


def test(identifier=""):
    """ test exection """
    event = {}
    event['website-metadata-tablename'] = 'steve-manifest-websiteMetadata470E321C-1NPIJYOXUCVHU'
    event = run(event, {})
    print(event)
