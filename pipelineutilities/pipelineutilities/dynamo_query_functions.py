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


def get_item_record(table_name: str, item_id: str) -> dict:
    """ Query Item record from dynamo based on item_id """
    results = {}
    if item_id:
        pk = 'ITEM#' + format_key_value(item_id)
        sk = pk
        try:
            table = boto3.resource('dynamodb').Table(table_name)
            response = table.get_item(Key={'PK': pk, 'SK': sk})
            results = response.get('Item', {})
        except ClientError as ce:
            capture_exception(ce)
    return results


def get_file_record(table_name: str, file_id: str) -> dict:
    """ Query Item record from dynamo based on item_id """
    results = {}
    if file_id:
        pk = 'FILE'
        sk = 'FILE#' + format_key_value(file_id)
        try:
            table = boto3.resource('dynamodb').Table(table_name)
            response = table.get_item(Key={'PK': pk, 'SK': sk})
            results = response.get('Item', {})
        except ClientError as ce:
            capture_exception(ce)
    return results


def get_file_to_process_record(table_name: str, destination_file_path: str) -> dict:
    """ Query FileToProcess record from dynamo based on destination_file_path """
    results = {}
    if destination_file_path:
        pk = 'FILETOPROCESS'
        sk = 'FILEPATH#' + format_key_value(destination_file_path)
        try:
            table = boto3.resource('dynamodb').Table(table_name)
            response = table.get_item(Key={'PK': pk, 'SK': sk})
            results = response.get('Item', {})
        except ClientError as ce:
            capture_exception(ce)
    return results


def get_all_file_to_process_records_by_storage_system(table_name: str, storage_system: str, type_of_data: str = None) -> dict:
    results = {}
    if storage_system:
        GSI1PK = 'FILETOPROCESS'
        GSI1SK = 'FILESYSTEM#' + format_key_value(storage_system)
        if type_of_data:
            GSI1SK += '#' + format_key_value(type_of_data)
        index = 'GSI1'
        kwargs = {'IndexName': index}
        kwargs['KeyConditionExpression'] = Key('GSI1PK').eq(GSI1PK) & Key('GSI1SK').begins_with(GSI1SK)
        kwargs['ProjectionExpression'] = 'id, filePath, dateModifiedInDynamo'
        try:
            while True:
                table = boto3.resource('dynamodb').Table(table_name)
                response = table.query(**kwargs)
                for item in response.get('Items', []):
                    new_node = {'filePath': item.get('filePath', ''), 'dateModifiedInDynamo': item.get('dateModifiedInDynamo')}
                    results[item.get('id')] = new_node
                if response.get('LastEvaluatedKey'):
                    kwargs['ExclusiveStartKey'] = response.get('LastEvaluatedKey')
                else:
                    break
        except ClientError as ce:
            capture_exception(ce)
    return results


def get_all_parent_override_records(table_name: str) -> dict:
    results = {}
    if table_name:
        GSI1PK = 'PARENTOVERRIDE'
        index = 'GSI1'
        kwargs = {'IndexName': index}
        kwargs['KeyConditionExpression'] = Key('GSI1PK').eq(GSI1PK)
        kwargs['ProjectionExpression'] = '#id, #parentId, #sequence'
        kwargs['ExpressionAttributeNames'] = {'#id': 'id', '#parentId': 'parentId', '#sequence': 'sequence'}  # Use this format when dealing with fields using DynamoDB key words
        try:
            while True:
                table = boto3.resource('dynamodb').Table(table_name)
                response = table.query(**kwargs)
                for item in response.get('Items', []):
                    new_node = {'parentId': item.get('parentId', ''), 'sequence': int(item.get('sequence', 0))}
                    results[item.get('id')] = new_node
                if response.get('LastEvaluatedKey'):
                    kwargs['ExclusiveStartKey'] = response.get('LastEvaluatedKey')
                else:
                    break
        except ClientError as ce:
            capture_exception(ce)
    return results
