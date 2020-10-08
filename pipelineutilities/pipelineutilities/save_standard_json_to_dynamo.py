"""
Save standard.json to Dynamo
"""

import boto3
from datetime import datetime, timedelta
# from record_files_needing_processed import FilesNeedingProcessed
from validate_json import validate_standard_json
from sentry_sdk import capture_exception
from botocore.exceptions import ClientError


class SaveStandardJsonToDynamo():
    """ Save Standard Json to Dynamo """
    def __init__(self, config: dict):
        self.config = config

    def save_standard_json(self, standard_json: dict, export_all_files_flag: bool = False) -> bool:
        """ First, validate the standard_json.  If this is the first time this standard_json is being saved,
            we will need to make sure all images and related files get processed.
            We next call a process to record files needing processed.
            We then save the standard_json. """
        success_flag = False
        if validate_standard_json(standard_json):
            if "id" in standard_json:
                # Need a way to query Dynamo to see if this has been saved before
                standard_json_already_exists_flag = False
                if not export_all_files_flag:
                    export_all_files_flag = (not standard_json_already_exists_flag)
                # files_needing_processed_class = FilesNeedingProcessed(self.config)
                # if files_needing_processed_class.record_files_needing_processed(standard_json, export_all_files_flag):
                #     success_flag = _save_json_to_s3(config['process-bucket'], key_name, standard_json)
                success_flag = self._save_json_to_dynamo(standard_json, _get_expire_time(3))
        return success_flag

    def _save_json_to_dynamo(self, standard_json: dict, expire_time: int) -> bool:
        """ Save each item recursively to dynamo then save root """
        success_flag = True
        if "items" in standard_json:
            for item in standard_json['items']:
                if item.get("level") != "file":
                    self._save_json_to_dynamo(item, expire_time)
        new_dict = {i: standard_json[i] for i in standard_json if i != 'items'}
        if 'expireTime' not in new_dict:
            new_dict['expireTime'] = expire_time
        try:
            standard_json_table = boto3.resource('dynamodb').Table(self.config['standard-json-tablename'])
            standard_json_table.put_item(Item=new_dict)
        except ClientError as ce:
            success_flag = False
            capture_exception(ce)
            print(f"Error saving to {self.config['standard-json-tablename']} table - {ce.response['Error']['Code']} - {ce.response['Error']['Message']}")
        return success_flag


def _get_expire_time(days_in_future: int = 3) -> int:
    """ Return Unix timestamp of now plus days_in_future """
    return int(datetime.timestamp(datetime.now() + timedelta(days=days_in_future)))
