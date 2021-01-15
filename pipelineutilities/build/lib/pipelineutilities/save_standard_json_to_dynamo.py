"""
Save standard.json to Dynamo
"""

from datetime import datetime, timedelta
from validate_json import validate_standard_json
from sentry_sdk import capture_exception
from botocore.exceptions import ClientError
from save_json_to_dynamo import SaveJsonToDynamo
from s3_helpers import read_s3_json, write_s3_json
import os
import boto3
from boto3.dynamodb.conditions import Key
from record_files_needing_processed import FilesNeedingProcessed
from dynamo_helpers import add_item_keys, add_file_group_keys


class SaveStandardJsonToDynamo():
    """ Save Standard Json to Dynamo """

    def __init__(self, config: dict):
        self.config = config
        self.related_ids_updated = False
        self.table_name = self.config.get('website-metadata-tablename')
        self.local = config.get('local', True)
        self.related_ids = self._read_related_ids()
        self.dynamo_table_available = self._init_dynamo_table()  # TODO: remove this when we move FilesNeedingProcessed to Museum and Curate exports

    def save_standard_json(self, standard_json: dict, export_all_files_flag: bool = False) -> bool:
        """ First, validate the standard_json.  If this is the first time this standard_json is being saved,
            we will need to make sure all images and related files get processed.
            We next call a process to record files needing processed.
            We then save the standard_json. """
        success_flag = False
        if validate_standard_json(standard_json):
            if "id" in standard_json:
                standard_json = add_item_keys(standard_json)
                # TODO remove files_needing_processed and move this to Google_Export and Curate_Export only
                if not export_all_files_flag:
                    export_all_files_flag = not self._id_exists_in_dynamo(standard_json['PK'])
                files_needing_processed_class = FilesNeedingProcessed(self.config)
                files_needing_processed_class.record_files_needing_processed(standard_json, export_all_files_flag)
                success_flag = self._save_json_to_dynamo(standard_json)
                if self.related_ids_updated:
                    self._save_related_ids()
        return success_flag

    def _save_json_to_dynamo(self, standard_json: dict) -> bool:
        """ Save each item recursively to dynamo then save root """
        success_flag = True
        if "items" in standard_json:
            for item in standard_json['items']:
                if item.get("level") != "file":
                    self._save_json_to_dynamo(item)
        if "childIds" in standard_json:
            self._append_related_ids(standard_json)
        standard_json = self._optionally_update_parent_id(standard_json)
        new_dict = {i: standard_json[i] for i in standard_json if i != 'items'}
        if standard_json.get("sourceSystem", "") in self.config.get("source-systems-requiring-metadata-expire-time"):
            new_dict['expireTime'] = int(datetime.timestamp(datetime.now() + timedelta(days=int(self.config.get('metadata-time-to-live-days', 3)))))
        new_dict = add_item_keys(new_dict)
        try:
            save_json_to_dynamo_class = SaveJsonToDynamo(self.config, self.table_name)
            record_inserted_flag = save_json_to_dynamo_class.save_json_to_dynamo(new_dict)
            if record_inserted_flag is None:
                success_flag = False
            if record_inserted_flag and new_dict.get('objectFileGroupId'):
                file_group_record = self._build_file_group_record(new_dict)
                self._save_file_group_record(file_group_record)
        except ClientError as ce:
            success_flag = False
            capture_exception(ce)
            print(f"Error saving to {self.table_name} table - {ce.response['Error']['Code']} - {ce.response['Error']['Message']}")
        return success_flag

    def _save_file_group_record(self, file_group_record: dict) -> bool:
        """ Save the File Group record to DynamoDB """
        success_flag = True
        if file_group_record:
            save_json_to_dynamo_class = SaveJsonToDynamo(self.config, self.table_name)
            save_json_to_dynamo_class.save_json_to_dynamo(file_group_record)
        return success_flag

    def _build_file_group_record(self, standard_json: dict) -> dict:
        """ Build the File Group record to be saved into DynamoDB"""
        file_group_record = {}
        file_group_record['objectFileGroupId'] = standard_json.get('objectFileGroupId')
        file_group_record = add_file_group_keys(file_group_record)
        return file_group_record

    def _read_related_ids(self) -> dict:
        """ Read related_ids.json into local dictionary """
        related_ids = {}
        if not self.local:
            bucket = self.config.get("process-bucket")
            s3_key = self._get_related_ids_s3_key()
            related_ids = read_s3_json(bucket, s3_key)
        return related_ids

    def _append_related_ids(self, standard_json) -> dict:
        """ update local dictionary with related ids """
        for related_id in standard_json.get("childIds", []):
            node = {"parentId": standard_json.get("id"), "sequence": related_id.get("sequence")}
            self.related_ids[related_id.get("id")] = node
            self.related_ids_updated = True
        return self.related_ids

    def _save_related_ids(self):
        """ save related_ids dictionary to related_ids.json on s3 """
        if self.related_ids_updated:
            bucket = self.config.get("process-bucket")
            s3_key = self._get_related_ids_s3_key()
            write_s3_json(bucket, s3_key, self.related_ids)
            self.related_ids_updated = False

    def _get_related_ids_s3_key(self) -> str:
        pipeline_control_folder = self.config.get("pipeline-control-folder")
        related_ids_file = self.config.get("related-ids-file")
        return os.path.join(pipeline_control_folder, related_ids_file)

    def _optionally_update_parent_id(self, standard_json: dict) -> dict:
        if standard_json["id"] in self.related_ids:
            standard_json["parentId"] = self.related_ids[standard_json["id"]].get("parentId")
            standard_json["sequence"] = self.related_ids[standard_json["id"]].get("sequence")
        return standard_json

    def _init_dynamo_table(self) -> bool:  # TODO: remove this when we move FilesNeedingProcessed to Museum and Curate exports
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

    def _id_exists_in_dynamo(self, id: str) -> bool:  # TODO: remove this when we move FilesNeedingProcessed to Museum and Curate exports
        """ Check to see if the id exists in dynamo """
        success_flag = False
        if self.config.get('local'):
            return False
        if self.dynamo_table_available:
            try:
                result = self.table.query(KeyConditionExpression=Key("PK").eq(id))
                if len(result.get('Items', [])) > 0:
                    success_flag = True
            except ClientError as ce:
                capture_exception(ce)
                print(f"save_standard_json_to_dynamo._id_exists_in_dynamo Error querying {self.table_name} table - {ce.response['Error']['Code']} - {ce.response['Error']['Message']}")
        return success_flag
