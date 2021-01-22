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

    def save_json_to_dynamo(self, json_dict: dict, save_only_new_records: bool = False) -> bool:
        """ Save json information to dynamo.
            If save_only_new_records is True, only save the record if the Primary Key (PK + SK) does not already exist
            Return values include:
                True - record was inserted
                False - record was updated
                None - no database action took place. """
        if not self.dynamo_table_available:
            return None
        record_inserted_flag = None
        json_dict = _serialize_json(json_dict)
        condition_expression = None
        if save_only_new_records:
            condition_expression = 'attribute_not_exists(PK) AND attribute_not_exists(SK)'
        kwargs = _build_put_item_parameters(json_dict, "ALL_OLD", condition_expression)
        try:
            results = self.table.put_item(**kwargs)
            if results.get('Attributes'):
                record_inserted_flag = False  # record was successfully updated
            else:
                record_inserted_flag = True  # record was inserted
        except ClientError as ce:
            if ce.response['Error']['Code'] != 'ConditionalCheckFailedException':
                capture_exception(ce)
                print(f"save_json_to_dynamo.py/save_json_to_dynamo Error saving to {self.table_name} table - {ce.response['Error']['Code']} - {ce.response['Error']['Message']}")
        return record_inserted_flag


def _build_put_item_parameters(json_dict: dict, return_values: str = None, condition_expression: str = None) -> dict:
    """ Build the parameters for put_item using passed parameters """
    put_parameters = {'Item': json_dict}
    if return_values:
        put_parameters['ReturnValues'] = return_values
    if condition_expression:
        put_parameters['ConditionExpression'] = condition_expression
    return put_parameters


# NOTE: I am keeping this here for now because in a couple of days, I want to come back and implement an update, and I think this will be a good start on that.
# def update_dynamo_record(self, json_dict: dict, condition_expression: str = None) -> bool:
#     """ update dynamo record, requires json_dict to contain PK and SK """
#     if not self.dynamo_table_available:
#         return None
#     update_expression, update_values = _get_update_params(json_dict)
#     response = self.table.update_item(
#         Key={'PK': json_dict['PK'], 'SK': json_dict['SK']},
#         UpdateExpression=update_expression,
#         ExpressionAttributeValues=dict(update_values),
#         ConditionExpression=condition_expression
#     )
#     return response


# def _get_update_params(json_dict: dict):
#     """ Given a dictionary, generate an update expression and a dict of values to update a dynamo table. """
#     update_expression = ["set "]
#     update_values = dict()
#     for key, val in json_dict.items():
#         update_expression.append(f" {key} = :{key},")
#         update_values[f":{key}"] = val
#     return "".join(update_expression)[:-1], update_values


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
