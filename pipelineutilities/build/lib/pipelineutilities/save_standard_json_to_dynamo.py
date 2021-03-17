"""
Save standard.json to Dynamo
"""

import boto3
from datetime import datetime, timedelta
from validate_json import validate_standard_json
from sentry_sdk import capture_exception
from botocore.exceptions import ClientError
from save_json_to_dynamo import SaveJsonToDynamo
from s3_helpers import read_s3_json, write_s3_json
import os
from dynamo_helpers import add_item_keys, add_website_item_keys, add_file_keys
from dynamo_save_functions import save_parent_override_record, save_file_to_process_record, save_file_group_record


class SaveStandardJsonToDynamo():
    """ Save Standard Json to Dynamo """

    def __init__(self, config: dict, time_to_break: int = None):
        self.config = config
        self.related_ids_updated = False
        self.table_name = self.config.get('website-metadata-tablename')
        self.local = config.get('local', True)
        self.related_ids = self._read_related_ids()
        self.time_to_break = time_to_break
        if not self.local:
            self.table = boto3.resource('dynamodb').Table(self.table_name)
            self.save_json_to_dynamo_class = SaveJsonToDynamo(config, self.config['website-metadata-tablename'])

    def save_standard_json(self, standard_json: dict, save_only_new_records: bool = False) -> bool:
        """ First, validate the standard_json.  If this is the first time this standard_json is being saved,
            we will need to make sure all images and related files get processed.
            We next call a process to record files needing processed.
            We then save the standard_json. """
        success_flag = False
        if validate_standard_json(standard_json) and not self.local:
            if "id" in standard_json:
                standard_json = add_item_keys(standard_json)
                success_flag = self._save_json_to_dynamo(standard_json, save_only_new_records)
                if self.related_ids_updated:
                    self._save_related_ids()
        return success_flag

    def _save_json_to_dynamo(self, standard_json: dict, save_only_new_records: bool = False) -> bool:  # noqa: C901
        """ Save each item recursively to dynamo then save root """
        success_flag = True
        if "items" in standard_json:
            for item in standard_json['items']:
                if item.get("level") != "file" and (not self.time_to_break or datetime.now() < self.time_to_break):
                    self._save_json_to_dynamo(item)
                if item.get("level") == 'file' and item.get('storageSystem', '') == 'Uri':
                    self._save_special_file_record(item)
        if "childIds" in standard_json:
            self._append_related_ids(standard_json)
        standard_json = self._optionally_update_parent_id(standard_json)
        new_dict = {i: standard_json[i] for i in standard_json if i != 'items'}
        if standard_json.get("sourceSystem", "") in self.config.get("source-systems-requiring-metadata-expire-time"):
            new_dict['expireTime'] = int(datetime.timestamp(datetime.now() + timedelta(days=int(self.config.get('metadata-time-to-live-days', 3)))))
        new_dict = add_item_keys(new_dict)
        if self.time_to_break and datetime.now() > self.time_to_break:
            return False
        try:
            with self.table.batch_writer() as batch:
                batch.put_item(Item=new_dict)
                if new_dict.get('parentId') == 'root':  # add WebsiteItem record to dynamo for all root items
                    webiste_item_record = {'id': new_dict['id'], 'websiteId': 'Marble'}
                    webiste_item_record = add_website_item_keys(webiste_item_record)
                    batch.put_item(Item=webiste_item_record)
        except ClientError as ce:
            success_flag = False
            capture_exception(ce)
            print(f"Error saving to {self.table_name} table - {ce.response['Error']['Code']} - {ce.response['Error']['Message']}")
        return success_flag

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
            if not self.local:
                save_parent_override_record(self.table_name, related_id.get("id"), standard_json.get("id"), related_id.get("sequence"))  # added to save to dynamo
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

    def _save_special_file_record(self, standard_json: dict):
        """ Files are automatically stored elsewhere for Curate, Museum, and S3, but not for Uri """
        if not self.local and standard_json.get('level') == 'file' and standard_json.get('storageSystem') == 'Uri':
            standard_json = add_file_keys(standard_json)
            self.save_json_to_dynamo_class.save_json_to_dynamo(standard_json)
            save_file_to_process_record(self.config['website-metadata-tablename'], standard_json)
            save_file_group_record(self.config['website-metadata-tablename'], standard_json.get('objectFileGroupId'), standard_json.get('storageSystem'), standard_json.get('typeOfData'))
