""" handler """

import _set_path  # noqa
import boto3
from boto3.dynamodb.conditions import Key, Attr
from pathlib import Path
import csv


def find_pdf_file_records(table_name: str, image_server_bucket_name: str, destination_bucket_name: str):
    pk = 'FILE'
    sk = 'FILE#'
    kwargs = {}
    kwargs['KeyConditionExpression'] = Key('PK').eq(pk) & Key('SK').begins_with(sk)
    kwargs['ProjectionExpression'] = 'PK, SK, id, objectGroupFileId, mediaGroupId, dateModifiedInDynamo, filePath'
    kwargs['FilterExpression'] = Attr("filePath").contains(".pdf")

    with open('pdf_file_records.csv', 'w') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=' ')
        with open('copy_pdf_files.sh', 'w') as copy_files:
            # copy_files_writer = csv.writer(copy_files, delimiter=' ')
            while True:
                results = query_dynamo_records(table_name, **kwargs)
                for record in results.get('Items', []):
                    csv_writer.writerow([record.get('PK'), record.get('SK'), record.get('id'), record.get('objectFileGroupId'), record.get('dateModifiedInDynamo'), record.get('filePath')])
                    file_path_minus_file_name = str(Path(record.get('filePath')).parent)
                    copy_from_location = 's3://' + image_server_bucket_name + '/' + file_path_minus_file_name
                    copy_to_location = 's3://' + destination_bucket_name + '/public-access/media/' + file_path_minus_file_name.replace('digital/MARBLE-images/', '').replace('/PDF', '')
                    copy_files.write('aws s3 sync {0} {1}'.format(copy_from_location, copy_to_location) + ' --exclude *.tif --exclude *.tiff --exclude ._* --include *.pdf \n')
                if results.get('LastEvaluatedKey'):
                    kwargs['ExclusiveStartKey'] = results.get('LastEvaluatedKey')
                else:
                    break
    return


def query_dynamo_records(table_name: str, **kwargs) -> dict:
    """ very generic dynamo query """
    response = {}
    table = boto3.resource('dynamodb').Table(table_name)
    response = table.query(**kwargs)
    return response

# setup:
# aws-vault exec testlibnd-superAdmin
# python -c 'from find_pdf_file_records import *; test()'

# testing:
# python 'run_all_tests.py'


def test(identifier=""):
    """ test exection """
    # testlibnd_tables = [
    #     'steve-manifest-websiteMetadata470E321C-1D6R3LX7EI284',
    #     'jon-test-manifest-websiteMetadata470E321C-ZCSU70JC12I0',
    #     'jon-prod-manifest-websiteMetadata470E321C-8NG755QB2S5I',
    #     'mlk-manifest-websiteMetadata470E321C-EU6Z5BXP1WVB',
    #     'sm-test-manifest-websiteMetadata470E321C-E5FXU8HUIWQG',
    #     'sm-prod-manifest-websiteMetadata470E321C-HO7FZQXZXI8M',
    #     'testlib-prod-manifest-websiteMetadata470E321C-1XA9OOG7PJWEE',
    #     'testlib-test-manifest-websiteMetadata470E321C-1XQ2EFEWM3UXZ'
    # ]
    # libnd_tables = [
    #     'marbleb-prod-manifest-websiteMetadata470E321C-5EJSG31E16Z7',
    #     'marbleb-test-manifest-websiteMetadata470E321C-JJG277N1OMMC'
    # ]

    event = find_pdf_file_records('marbleb-prod-manifest-websiteMetadata470E321C-5EJSG31E16Z7', 'marbleb-prod-foundation-publicbucketa6745c15-cn2zeuo35i11', 'libnd-smb-marble')
