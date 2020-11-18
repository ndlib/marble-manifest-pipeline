"""
Save json to Dynamo
"""

import boto3
from datetime import datetime, timedelta, date
from sentry_sdk import capture_exception
from botocore.exceptions import ClientError


class SaveJsonToDynamo():
    """ Save Json to Dynamo """
    def __init__(self, config: dict, table_name: str, expire_time_days_in_future: int = 3, dynamo_records_expire_time: int = None):
        """ User may pass either expire_time_days_in_future to define time_to_live for dynamo records
            User could also optionally pass a specific expire time """
        self.table_name = table_name
        self.local = config.get('local', True)
        self.dynamo_table_available = self._init_dynamo_table()
        if dynamo_records_expire_time:
            self.expire_time = dynamo_records_expire_time
        else:
            self.expire_time = _get_expire_time(datetime.now(), int(expire_time_days_in_future))

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

    def save_json_to_dynamo(self, json_dict: dict, dynamo_record_expire_time: int = None) -> bool:
        """ Save json information to dynamo.  User may override default calculated expire_time if they choose. """
        if not self.dynamo_table_available:
            return False
        if not dynamo_record_expire_time:
            dynamo_record_expire_time = self.expire_time
        success_flag = True
        json_dict = _serialize_json(json_dict)
        if 'expireTime' not in json_dict:
            json_dict['expireTime'] = dynamo_record_expire_time
        try:
            self.table.put_item(Item=json_dict)
        except ClientError as ce:
            success_flag = False
            capture_exception(ce)
            print(f"Error saving to {self.table_name} table - {ce.response['Error']['Code']} - {ce.response['Error']['Message']}")
        return success_flag


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


def _get_expire_time(initial_datetime: datetime, days_in_future: int = 3) -> int:
    """ Return Unix timestamp of initial_datetime plus days_in_future days."""
    return int(datetime.timestamp(initial_datetime + timedelta(days=days_in_future)))
