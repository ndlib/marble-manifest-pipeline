"""
Save default file metadata to Dynamo
"""

from sentry_sdk import capture_exception
from botocore.exceptions import ClientError
import boto3
from boto3.dynamodb.conditions import Key


class SaveDefaultFileMetadataToDynamo():
    """ Save Default File Metadata to defaultFileMetadta Dynamo table """

    def __init__(self, config: dict):
        self.config = config
        self.table_name = self.config.get('default-file-metadata-tablename')
        self.local = config.get('local', True)
        self.dynamo_table_available = self._init_dynamo_table()

    def save_default_file_metadata(self, standard_json: dict) -> dict:
        """ Find the default file associated with this item.
            If there is one, save objectFileGroupId from that file, alsong with
            several fields from the item record.

            Eventually, we will need to only update copyright info if it doesn't already exist,
            which means we will need to update records instead of insert them. """
        new_node_to_save = {}
        default_file_node = self._find_default_file_node(standard_json)
        if default_file_node:
            new_node_from_stadard_json = self._create_new_node_from_standard_json(standard_json, default_file_node)
            existing_dynamo_record = self._get_existing_dynamo_record(standard_json['id'])
            new_node_to_save = self._merge_data_to_save(existing_dynamo_record, new_node_from_stadard_json)
            if not self._save_json_to_dynamo(new_node_to_save):
                new_node_to_save = {}
        return new_node_to_save

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

    def _find_default_file_node(self, standard_json: dict) -> dict:
        """ Find the first occurrance of a file item that is an immediate
            descendant of this node and has the thumbnail property set to true """
        file_node = {}
        for item in standard_json.get('items', []):
            if item.get('level', 'manifest') == 'file' and item.get('thumbnail', False):
                file_node = item
                break
        return file_node

    def _create_new_node_from_standard_json(self, standard_json: dict, default_file_node: dict) -> dict:
        """ Given the Standard Json for this item and the default_file_record,
            create a new node including information we want to store to dynamo. """
        new_node = {}
        new_node['id'] = standard_json['id']
        if standard_json.get('copyrightStatement'):
            new_node['copyrightStatement'] = standard_json['copyrightStatement']
        if standard_json.get('copyrightStatus'):
            new_node['copyrightStatus'] = standard_json['copyrightStatus']
        if standard_json.get('copyrightUrl'):
            new_node['copyrightUrl'] = standard_json['copyrightUrl']
        if standard_json.get('defaultFilePath'):
            new_node['defaultFilePath'] = standard_json['defaultFilePath']
        if default_file_node.get('objectFileGroupId'):
            new_node['objectFileGroupId'] = default_file_node['objectFileGroupId']
        return new_node

    def _get_existing_dynamo_record(self, id: str) -> dict:
        """ Read existing record from dynamo (if any), returning it"""
        dynamo_record = {}
        if self.config.get('local'):
            return False
        if self.dynamo_table_available:
            try:
                result = self.table.query(KeyConditionExpression=Key("id").eq(id))
                if len(result.get('Items', [])) > 0:
                    dynamo_record = result['Items'][0]
            except ClientError as ce:
                capture_exception(ce)
                print(f"Error querying {self.table_name} table - {ce.response['Error']['Code']} - {ce.response['Error']['Message']}")
        return dynamo_record

    def _merge_data_to_save(save, existing_dynamo_record: dict, new_node_from_standard_json: dict) -> dict:
        """ Merge data from existing dynamo record into new node from standard_json """
        node_to_save = new_node_from_standard_json.copy()
        for k, v in existing_dynamo_record.items():
            node_to_save[k] = v
        return node_to_save

    def _save_json_to_dynamo(self, new_node_to_save: dict) -> bool:
        """ Save the record to dynamo """
        success_flag = False
        try:
            self.table.put_item(Item=new_node_to_save)
        except ClientError as ce:
            success_flag = False
            capture_exception(ce)
            print(f"Error saving to {self.table_name} table - {ce.response['Error']['Code']} - {ce.response['Error']['Message']}")
        return success_flag
