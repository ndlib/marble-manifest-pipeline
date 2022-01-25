""" handler """

import _set_path  # noqa
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import os
from pathlib import Path
import sentry_sdk   # noqa: E402
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
from dynamo_query_functions import scan_dynamo_records, insert_supplemental_data_records


if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(dsn=os.environ['SENTRY_DSN'], integrations=[AwsLambdaIntegration()])


def run(event: dict, context: dict) -> dict:
    if event.get('deleteAllFileRecords', False):
        event['fileRecordsDeleted'] = delete_file_records(event.get('website-metadata-tablename'), event.get('deleteAllFileRecords', False))  # 2252 .xml files deleted
    if event.get('deleteAllFileToProcessRecords', False):
        event['fileToHarvestRecordsDeleted'] = delete_file_to_process_records(event.get('website-metadata-tablename'), event.get('deleteAllFileToProcessRecords', False))  # 5488 records deleted
    if event.get('deleteSpecificFileRecords', False):
        delete_specific_file_records(event.get('website-metadata-tablename'))
    if event.get('insertSupplementalDataRecords', False):
        event['supplementalDataRecordsInserted'] = insert_supplemental_data_records(event.get('website-metadata-tablename'), ['Aleph', 'ArchivesSpace', 'Curate'])
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


def clean_up_supplemental_data_records(table_name: str):
    """ Delete certain SupplementalData records for those records accidentally inserted based on file ids -- keep this code as a model for future potential needs """
    print("deleting SupplementalData records")
    pk = 'SUPPLEMENTALDATA'
    sk = 'ITEM#'
    kwargs = {}
    kwargs['IndexName'] = 'GSI1'
    kwargs['KeyConditionExpression'] = Key('GSI1PK').eq(pk) & Key('GSI1SK').begins_with(sk)
    kwargs['ProjectionExpression'] = 'PK, SK, sourceType'
    done = False
    records_deleted = 0
    table = boto3.resource('dynamodb').Table(table_name)
    while not done:
        results = query_dynamo_records(table_name, **kwargs)
        with table.batch_writer() as batch:
            for record in results.get('Items', []):
                file_extension = Path(record.get('PK')).suffix
                if file_extension and '.' in file_extension and file_extension.lower() in ['.pdf', '.tif', '.jpg']:  # '.jpg'
                    print('deleting record = ', record.get('PK'), record.get('SK'))
                    records_deleted += 1
                    batch.delete_item(Key={'PK': record.get('PK'), 'SK': record.get('SK')})
        if results.get('LastEvaluatedKey'):
            kwargs['ExclusiveStartKey'] = results.get('LastEvaluatedKey')
        else:
            done = True
    return records_deleted


def find_item_records_with_images(table_name: str):
    kwargs = {}
    kwargs['FilterExpression'] = Attr("TYPE").eq("Item") and Attr("imageGroupId").exists()
    kwargs['ProjectionExpression'] = 'id, imageGroupId'
    continued_count = 0
    record_count = 0
    unique_imageGroupIds_referenced = []
    while True:
        results = scan_dynamo_records(table_name, **kwargs)
        for record in results.get('Items', []):
            record_count += 1
            image_group_id = record.get('imageGroupId')
            if image_group_id:
                record_count += 1
                if image_group_id not in unique_imageGroupIds_referenced:
                    unique_imageGroupIds_referenced.append(image_group_id)

        if results.get('LastEvaluatedKey'):
            kwargs['ExclusiveStartKey'] = results.get('LastEvaluatedKey')
            continued_count += 1
        else:
            break
    print("continued_cound =", continued_count, "record_count =", record_count, len(unique_imageGroupIds_referenced))
    return unique_imageGroupIds_referenced


def delete_certain_image_records(table_name: str):
    """ Delete certain File records, depending on criteria below """
    print("deleting Image records")
    pk = 'IMAGE'
    sk = 'IMAGE#DIGITAL/MARBLE-IMAGES'
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
                batch.delete_item(Key={'PK': record.get('PK'), 'SK': record.get('SK')})
                records_deleted += 1
        if results.get('LastEvaluatedKey'):
            kwargs['ExclusiveStartKey'] = results.get('LastEvaluatedKey')
        else:
            done = True
    return records_deleted


def reset_file_to_process_records_to_reprocess_all_images(table_name: str) -> int:
    """ Reset FileToProcess records, removing lastProcessedDate """
    print("deleting FileToProcess records")
    pk = 'FILETOPROCESS'
    sk = 'FILEPATH#'
    kwargs = {}
    kwargs['KeyConditionExpression'] = Key('PK').eq(pk) & Key('SK').begins_with(sk)
    kwargs['ProjectionExpression'] = 'PK, SK, dateLastProcessed, GSI2SK'
    done = False
    records_updated = 0
    table = boto3.resource('dynamodb').Table(table_name)
    while not done:
        results = query_dynamo_records(table_name, **kwargs)
        #  note:  because batch_writer only supports PutItem and DeleteItem, and does not support UpdateItem, we must use the table object instead of the batch writer object
        for record in results.get('Items', []):
            if record.get('GSI2SK', '') != 'DATELASTPROCESSED#':
                table.update_item(
                    Key={'PK': record.get('PK'), 'SK': record.get('SK')},
                    UpdateExpression="set GSI2SK=:gsi2sk, dateLastProcessed=:dateLastProcessed",
                    ExpressionAttributeValues={':gsi2sk': 'DATELASTPROCESSED#', ':dateLastProcessed': ''},
                    ReturnValues="ALL_NEW"
                )
                records_updated += 1
        if results.get('LastEvaluatedKey'):
            kwargs['ExclusiveStartKey'] = results.get('LastEvaluatedKey')
        else:
            done = True
    return records_updated


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
# aws-vault exec testlibnd-superAdmin
# python -c 'from handler import *; test()'

# testing:
# python 'run_all_tests.py'


def test(identifier=""):
    """ test exection """
    # print(find_item_records_with_images('steve-manifest-websiteMetadata470E321C-1D6R3LX7EI284'))
    event = {}
    testlibnd_tables = [
        'steve-manifest-websiteMetadata470E321C-67TJ4WN9G68J'
        'sm-test-manifest-websiteMetadata470E321C-BGP52HX1S5IC',
        'sm-prod-manifest-websiteMetadata470E321C-7145C85FIFTW'
        'testlib-prod-manifest-websiteMetadata470E321C-1XA9OOG7PJWEE',
        'testlib-test-manifest-websiteMetadata470E321C-1XQ2EFEWM3UXZ'
    ]
    libnd_tables = [
        'marbleb-prod-manifest-websiteMetadata470E321C-5EJSG31E16Z7',
        'marbleb-test-manifest-websiteMetadata470E321C-JJG277N1OMMC'
    ]
    # tables_to_process = testlibnd_tables
    tables_to_process = libnd_tables

    # event['website-metadata-tablename'] = 'steve-manifest-websiteMetadata470E321C-1D6R3LX7EI284'
    # event['website-metadata-tablename'] = 'marbleb-prod-manifest-websiteMetadata470E321C-5EJSG31E16Z7'
    # event['website-metadata-tablename'] = 'marbleb-test-manifest-websiteMetadata470E321C-JJG277N1OMMC'
    event['deleteAllFileRecords'] = False
    event['deleteAllFileToProcessRecords'] = False
    event['deleteSpecificFileRecords'] = False
    event['insertSupplementalDataRecords'] = False

    # Do processing desired for all tables listed
    for table_name in tables_to_process:
        # event['website-metadata-tablename'] = table_name
        # event = run(event, {})
        # print(table_name, event)
        records_processed = reset_file_to_process_records_to_reprocess_all_images(table_name)
        print(table_name, " had ", records_processed, "records processed.")
