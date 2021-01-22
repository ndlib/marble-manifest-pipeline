"""
Read from Dynamo
"""

import boto3
from sentry_sdk import capture_exception
from botocore.exceptions import ClientError
from dynamo_helpers import format_key_value
from boto3.dynamodb.conditions import Key


class ReadFromDynamo():
    """ Save Json to Dynamo """
    def __init__(self, config: dict, table_name: str):
        """ Init now only needs config and table_name.  No knowledge of the table structure is needed. """
        self.table_name = table_name
        self.local = config.get('local', True)
        self.dynamo_table_available = self._init_dynamo_table()

    def _init_dynamo_table(self) -> bool:
        """ Create boto3 resource for dynamo table, returning a boolean flag indicating if this resource exists """
        success_flag = False
        if not self.local:
            try:
                self.table = boto3.resource('dynamodb').Table(self.table_name)
                success_flag = True
            except ClientError as ce:
                capture_exception(ce)
                print(f"Error saving to {self.table_name} table - {ce.response['Error']['Code']} - {ce.response['Error']['Message']}")
        return success_flag

    def query_from_dynamo(self, kwargs: dict) -> dict:
        """ Read from dynamo.
            Return response from DynamoDB """
        if not self.dynamo_table_available:
            return None
        try:
            results = self.table.query(**kwargs)
        except ClientError as ce:
            if ce.response['Error']['Code'] != 'ConditionalCheckFailedException':
                capture_exception(ce)
                print(f"save_json_to_dynamo.py/save_json_to_dynamo Error saving to {self.table_name} table - {ce.response['Error']['Code']} - {ce.response['Error']['Message']}")
        return results

    def read_items_to_harvest(self, source_system: str) -> dict:
        key_condition_expression = Key('PK').eq('ITEMTOHARVEST') & Key('SK').begins_with('SOURCESYSTEM#' + format_key_value(source_system))
        kwargs = {'KeyConditionExpression': key_condition_expression}
        response = self.query_from_dynamo(kwargs)
        if response.get('Items'):
            harvest_ids_list = get_string_list_from_items(response.get('Items'), 'harvestItemId')

        while 'LastEvaluatedKey' in response:
            kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
            response = self.query_from_dynamo(kwargs)
            harvest_ids_list.append(get_string_list_from_items(response.get('Items'), 'harvestItemId'))
        return harvest_ids_list


def get_string_list_from_items(items_json: dict, key_to_retrieve: str) -> str:
    str_list_to_return = []
    for item in items_json:
        str_list_to_return.append(item[key_to_retrieve])
    return str_list_to_return
