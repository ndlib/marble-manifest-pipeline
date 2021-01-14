"""
Save json to Dynamo
"""

import boto3
from datetime import datetime, date
from sentry_sdk import capture_exception
from botocore.exceptions import ClientError


class SaveJsonToDynamo():
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

    def save_json_to_dynamo(self, json_dict: dict) -> bool:
        """ Save json information to dynamo, return values include:
            True - record was inserted
            False - record was updated
            None - no database action took place. """
        if not self.dynamo_table_available:
            return None
        record_inserted_flag = None
        json_dict = _serialize_json(json_dict)
        try:
            # TODO: research using update_item.  This will require some pretty fundamental changes.  This would allow us to maintain dateAddedToDynamo.
            results = self.table.put_item(Item=json_dict, ReturnValues='ALL_OLD')
            if results.get('Attributes'):
                record_inserted_flag = False  # record was successfully updated
            else:
                record_inserted_flag = True
        except ClientError as ce:
            capture_exception(ce)
            print(f"save_json_to_dynamo.py/save_json_to_dynamo Error saving to {self.table_name} table - {ce.response['Error']['Code']} - {ce.response['Error']['Message']}")
        return record_inserted_flag


def _serialize_json(json_node: dict) -> dict:
    """ This fixes a problem when trying to save datetime information to dynamo """
    if isinstance(json_node, dict):
        for k, v in json_node.items():
            if isinstance(v, (datetime, date)):
                json_node[k] = v.isoformat()
            elif isinstance(v, list):
                json_node[k] = _serialize_json(v)
    elif isinstance(json_node, list):
        for node in json_node:
            node = _serialize_json(node)
    return json_node
