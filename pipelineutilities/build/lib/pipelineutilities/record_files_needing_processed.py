# record_files_needing_processed.py

import boto3
from datetime import datetime, timedelta
from pipelineutilities.s3_helpers import read_s3_json, write_s3_json


class FilesNeedingProcessed():
    """ This performs all Curate API processing """
    def __init__(self, config):
        self.config = config

    def record_files_needing_processed(self, standard_json: dict, export_all_files_flag: bool) -> bool:
        """ Save the dictionary of files needing processed to S3, appending it to the existing dictionary if one exists."""
        success_flag = True
        source_system = standard_json.get("sourceSystem", "")
        if source_system not in ("Curate", "EmbARK"):
            return success_flag
        files_dict = self._create_files_dict(standard_json, export_all_files_flag, None)
        bucket_name = self.config["process-bucket"]
        for key, value in files_dict.items():
            file_name = key + ".json"  # key will be similar to "curate" or "google" or "bendo"
            original_hash = _load_json_from_s3(bucket_name, file_name)
            original_hash.update(value)
            write_s3_json(bucket_name, file_name, original_hash)
            # print("files_needing_processed saved for: ", standard_json.get("id", ""), " in ", file_name, " in bucket ", bucket_name)
        return success_flag

    def _create_files_dict(self, standard_json: dict, export_all_files_flag: bool, export_since_date: datetime) -> list:
        """ If export_all_files_flag is True, we will export all records with level = "file",
            also, export all children files for all items with a createdDate or modifiedDate
            more recent than export_since_date"""
        image_list = {}
        if not export_since_date:
            export_since_date = self._default_export_since_date()
        self._optionally_append_to_file_list(standard_json, export_all_files_flag, export_since_date, image_list)
        return image_list

    def _optionally_append_to_file_list(self, standard_json: dict, export_all_files_flag: bool, export_since_date: datetime, image_dict: dict):
        """ If export_all_files_flag is True, we will export all records with level = "file",
            also, export all children files for all items with a createdDate or modifiedDate
            more recent than export_since_date"""
        if self._record_has_been_updated_recently(standard_json, export_since_date):
            export_all_files_flag = True
        if standard_json.get("level", "") == "file" and export_all_files_flag is True:
            if standard_json.get("filePath", "") > "":
                main_key = self._get_key_given_file_path(standard_json["filePath"])
                if main_key:
                    if main_key not in image_dict:
                        image_dict[main_key] = {}
                    image_dict[main_key][standard_json["filePath"]] = standard_json
        if "items" in standard_json:
            for item in standard_json["items"]:
                self._optionally_append_to_file_list(item, export_all_files_flag, export_since_date, image_dict)

    def _get_key_given_file_path(self, file_path: str) -> str:
        key = None
        if "https://curate.nd" in file_path or "http://curate.nd" in file_path:
            key = "curate"
        if "drive.google.com" in file_path:
            key = "google"
        if "bendo" in file_path:
            key = "bendo"
        return key

    def _default_export_since_date(self) -> datetime:
        return datetime.now() + timedelta(days=-2)

    def _record_has_been_updated_recently(self, standard_json: dict, export_since_date: datetime) -> bool:
        """ Note: dates will be passed in this format:  2018-11-16T00:00:00Z """
        if "modifiedDate" in standard_json:
            date_to_check = self._get_date_from_date_field(standard_json["modifiedDate"])
        elif "createdDate" in standard_json:
            date_to_check = self._get_date_from_date_field(standard_json["createdDate"])
        else:
            date_to_check = datetime.now()
        return (date_to_check >= export_since_date)

    def _get_date_from_date_field(self, date_field: str) -> datetime:
        try:
            date_to_check = datetime.strptime(date_field, '%Y-%m-%dT%H:%M:%SZ')
        except Exception:
            date_to_check = datetime.strptime(date_field, '%Y-%m-%dT%H:%M:%S.%fZ')
        return date_to_check


def _load_json_from_s3(s3_bucket, s3_path):
    try:
        return read_s3_json(s3_bucket, s3_path)
    except boto3.resource('s3').meta.client.exceptions.NoSuchKey:
        return {}
