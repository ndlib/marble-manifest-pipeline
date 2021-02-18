""" dynamo_query_functions.py
    This module will query specific types of records from DynamoDB
"""
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key  # , Attr
from sentry_sdk import capture_exception
from datetime import datetime, timedelta
from dynamo_helpers import format_key_value


def get_subject_term_record(table_name: str, uri: str) -> dict:
    """ Query SubjectTerm record from dynamo based on uri """
    results = {}
    if uri:
        pk = 'SUBJECTTERM'
        sk = 'URI#' + format_key_value(uri)
        try:
            table = boto3.resource('dynamodb').Table(table_name)
            response = table.get_item(Key={'PK': pk, 'SK': sk})
            results = response.get('Item', {})
        except ClientError as ce:
            capture_exception(ce)
    return results


def get_subject_terms_needing_expanded(table_name: str, days_before_expanding_terms_again: int = 30) -> list:
    """ Query SubjectTerm records from dynamo based on when they were last expanded """
    results = []
    if days_before_expanding_terms_again:
        appropriate_date = datetime.now() - timedelta(days=days_before_expanding_terms_again)
        GSI2PK = 'SUBJECTTERM'
        GSI2SK = 'LASTHARVESTDATE#' + format_key_value(appropriate_date.isoformat())
        index = 'GSI2'
        kwargs = {'IndexName': index}
        kwargs['KeyConditionExpression'] = Key('GSI2PK').eq(GSI2PK) & Key('GSI2SK').lt(GSI2SK)
        results = []
        try:
            while True:
                table = boto3.resource('dynamodb').Table(table_name)
                response = table.query(**kwargs)
                results.extend(response.get('Items', []))
                if response.get('LastEvaluatedKey'):
                    kwargs['ExclusiveStartKey'] = response.get('LastEvaluatedKey')
                else:
                    break
        except ClientError as ce:
            capture_exception(ce)
    return results
