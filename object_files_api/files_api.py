""" Files API """
import boto3
import os
import io
from datetime import date, datetime, timedelta
import json
import time
from s3_helpers import write_s3_json, read_s3_json, delete_s3_key
from api_helpers import json_serial
from search_files import crawl_available_files
from sentry_sdk import capture_exception
from botocore.exceptions import ClientError


class FilesApi():
    def __init__(self, event, config):
        self.event = event
        self.event['local'] = self.event.get('local', False)
        self.config = config
        self.local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"
        self.time_to_break = datetime.now() + timedelta(seconds=config['seconds-to-allow-for-processing'])
        if self.config.get('test', False):
            self.directory = os.path.join(os.path.dirname(__file__), 'test')
        else:
            self.directory = os.path.join(os.path.dirname(__file__), 'cache')
        self.start_time = time.time()
        self.dynamo_table_available = self._init_dynamo_table()
        self.resumption_filename = 'file_objects_list_partially_processed.json'

    def save_files_details(self):
        """ This will crawl available files, then loop through the file listing, saving each to dynamo """
        if self.event['objectFilesApi_execution_count'] == 1:
            all_files_listing = self._crawl_available_files_from_s3_or_cache(True)
        else:
            all_files_listing = self._resume_execution()
        file_objects = []
        processing_complete = True
        for key, value in all_files_listing.items():
            if not value.get('recordProcessedFlag', False):
                file_objects.extend(self._save_file_objects_per_collection(value))
                value['recordProcessedFlag'] = True
                print("saved", len(value.get('files', [])), "files for collection: ", key, int(time.time() - self.start_time), 'seconds.')
            if datetime.now() >= self.time_to_break:
                self._save_progress(all_files_listing)
                processing_complete = False
                break
        if processing_complete:
            self._clean_up_when_done()
            self.event['objectFilesApiComplete'] = True
        if self.event['local']:
            self._cache_s3_call(os.path.join(self.directory, "file_objects.json"), file_objects)
        else:
            write_s3_json(self.config['manifest-server-bucket'], 'objectFiles/all/index.json', file_objects)
        return file_objects

    def _init_dynamo_table(self) -> bool:
        """ Create boto3 resource for dynamo table """
        success_flag = False
        if not self.event['local']:
            try:
                self.files_table = boto3.resource('dynamodb').Table(self.config['files-tablename'])
                success_flag = True
            except ClientError as ce:
                capture_exception(ce)
                print(f"Error saving to {self.config['files-tablename']} table - {ce.response['Error']['Code']} - {ce.response['Error']['Message']}")
        return success_flag

    def _save_progress(self, all_files_listing: dict):
        """ This is used to save progress in order to resume execution later """
        if self.event['local']:
            cache_file_name = os.path.join(self.directory, self.resumption_filename)
            self._cache_s3_call(self, cache_file_name, all_files_listing)
        else:
            s3_key = os.path.join(self.config['pipeline-control-folder'], self.resumption_filename)
            write_s3_json(self.config['process-bucket'], s3_key, all_files_listing)

    def _resume_execution(self) -> list:
        """ This re-loads the all_files_listing saved as part of _save_progress in order to resume execution """
        all_files_listing = []
        if self.event['local']:
            cache_file_name = os.path.join(self.directory, self.resumption_filename)
            if os.path.exists(cache_file_name):
                with io.open(cache_file_name, 'r', encoding='utf-8') as json_file:
                    all_files_listing = json.load(json_file)
        else:
            s3_key = os.path.join(self.config['pipeline-control-folder'], self.resumption_filename)
            all_files_listing = read_s3_json(self.config['process-bucket'], s3_key)
        return all_files_listing

    def _clean_up_when_done(self):
        """ This deletes work-in-progress files """
        if self.event['local']:
            cache_file_name = os.path.join(self.directory, self.resumption_filename)
            if os.path.exists(cache_file_name):
                os.remove(cache_file_name)
        else:
            s3_key = os.path.join(self.config['pipeline-control-folder'], self.resumption_filename)
            delete_s3_key(self.config['process-bucket'], s3_key)

    def _save_file_objects_per_collection(self, collection_json: dict) -> list:
        """ Loop through every file in a collection and save each record into dynamo """
        i = 0
        collection_list = []
        for file_info in collection_json.get('files', []):
            i += 1
            my_json = dict(file_info)
            my_json['sequence'] = i
            my_json['id'] = my_json.get('key', '')
            collection_list.append(my_json)
            if self.dynamo_table_available:
                self._save_json_to_dynamo(my_json)
        return collection_list

    def _cache_s3_call(self, file_name: str, objects: dict):
        """ Save json file locally """
        with open(file_name, 'w') as outfile:
            json.dump(objects, outfile, default=json_serial, sort_keys=True, indent=2)

    def _crawl_available_files_from_s3_or_cache(self, force_use_s3: bool = False) -> dict:
        """ Find all related files, whether from querying S3 or loading from a local json file. """
        cache_file = os.path.join(self.directory, 'crawl_available_files_cache.json')
        if force_use_s3 or (not self.config.get("test", False) and not self.config.get('local', False)):
            objects = crawl_available_files(self.config)
            if self.config.get('local', False):
                self._cache_s3_call(cache_file, objects)
            return objects
        elif os.path.exists(cache_file):
            with io.open(cache_file, 'r', encoding='utf-8') as json_file:
                return json.load(json_file)
        else:
            return {}

    def _save_json_to_dynamo(self, files_json: dict) -> bool:
        """ Save each item to dynamo """
        success_flag = True
        files_json = _serialize_json(files_json)
        if 'expireTime' not in files_json:
            files_json['expireTime'] = _get_expire_time(datetime.now(), 3)
        if self.dynamo_table_available:
            try:
                self.files_table.put_item(Item=files_json)
            except ClientError as ce:
                success_flag = False
                capture_exception(ce)
                print(f"Error saving to {self.config['files-tablename']} table - {ce.response['Error']['Code']} - {ce.response['Error']['Message']}")
        else:
            success_flag = False
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
