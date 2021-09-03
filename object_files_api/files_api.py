""" Files API """
import boto3
import os
import io
from datetime import datetime, timedelta
import json
import time
from s3_helpers import write_s3_json, read_s3_json, delete_s3_key
from api_helpers import json_serial
from search_files import crawl_available_files, update_pdf_fields
from dynamo_helpers import add_file_to_process_keys, add_file_group_keys, get_iso_date_as_string, add_image_group_keys, add_media_group_keys, add_media_keys, add_image_keys
from dynamo_save_functions import save_file_system_record
from add_files_to_json_object import change_file_extensions_to_tif
from pipelineutilities.dynamo_query_functions import get_all_file_to_process_records_by_storage_system


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
        self.table_name = self.config.get('website-metadata-tablename', '')
        self.resumption_filename = 'file_objects_list_partially_processed.json'
        if not self.event['local']:
            save_file_system_record(self.table_name, 'S3', 'Marble content bucket')
            save_file_system_record(self.table_name, 'S3', 'Multimedia bucket')
        self.file_to_process_records_in_dynamo = {}
        if not self.config.get('local', True):
            print("pulling file_to_process_records from dynamo")
            self.file_to_process_records_in_dynamo = get_all_file_to_process_records_by_storage_system(self.config.get('website-metadata-tablename', ''), 'S3')
            self.table = boto3.resource('dynamodb').Table(self.table_name)
        self.config['forceSaveCrawlAvailableFiles'] = False

    def save_files_details(self):
        """ This will crawl available files, then loop through the file listing, saving each to dynamo """
        if self.event['objectFilesApi_execution_count'] == 1:
            marble_files = self._crawl_available_files_from_s3_or_cache(self.config['marble-content-bucket'], True)
            # rbsc_files = self._crawl_available_files_from_s3_or_cache(self.config['rbsc-image-bucket'], True)  # save in case we need to crawl the RBSC bucket ever again
            # all_files_listing = {**rbsc_files, **marble_files}
            all_files_listing = {**marble_files}
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

    def _save_file_objects_per_collection(self, collection_json: dict) -> list:  # noqa:  C901
        """ Loop through every file in a collection and save each record into dynamo """
        i = 0
        collection_list = []
        object_group_ids = []
        image_group_ids = []
        media_group_ids = []
        for file_info in collection_json.get('files', []):
            i += 1
            my_json = dict(file_info)
            my_json['sequence'] = i
            my_json['id'] = my_json.get('key', '')
            my_json['copyrightStatus'] = "Public domain"
            if 'copyright' in my_json.get('key', '').lower():  # If the word "copyright" exists in the folder structure, this file is Copyrighted
                my_json['copyrightStatus'] = "Copyright"
            if 'mediaGroupId' not in my_json and 'imageGroupId' not in my_json:
                continue  # We must have either mediaGroupId or imageGroupId or we can't process this record.
            if 'mediaGroupId' in my_json:  # is_media_file(self.config.get('media-file-extensions', []), my_json.get('id')):
                my_json = add_media_keys(my_json, self.config.get('media-server-base-url', ''))
            else:
                update_pdf_fields(my_json)
                my_json = change_file_extensions_to_tif(my_json, self.config.get("file-extensions-to-protect-from-changing-to-tif", []))
                my_json = add_image_keys(my_json, self.config.get('image-server-base-url', ''))
            my_json['typeOfData'] = my_json.get('typeOfData', 'Marble content bucket')
            collection_list.append(my_json)
            my_json['storageSystem'] = my_json.get('storageSystem', 'S3')
            if 'sourceFilePath' not in my_json:
                my_json['sourceFilePath'] = my_json.get('path', '')  # only add if this does not already exist
            if not self.config.get('local', False):
                with self.table.batch_writer() as batch:
                    batch.put_item(Item=my_json)
                    item_id = my_json.get('id')
                    if self.config.get('folder_exposed_through_cdn') not in my_json.get('id'):  # nothing in the folder exposed through cdn should have image processing done for it.
                        if self.event.get('exportAllFilesFlag', False) or item_id not in self.file_to_process_records_in_dynamo or my_json.get('modifiedDate', '') > self.file_to_process_records_in_dynamo[item_id].get('dateModifiedInDynamo', ''):  # noqa: #501
                            file_to_process_json = dict(my_json)
                            file_to_process_json = add_file_to_process_keys(file_to_process_json)
                            batch.put_item(Item=file_to_process_json)
                            collection_list.append(file_to_process_json)
                    # Only insert Group records once
                    object_group_id = my_json.get('objectFileGroupId')
                    if object_group_id and object_group_id not in object_group_ids:  # This will be removed once we transition to imageGroupId and mediaGroupId
                        file_group_record = {'objectFileGroupId': object_group_id}
                        file_group_record['storageSystem'] = my_json.get('storageSystem')
                        file_group_record['typeOfData'] = my_json.get('typeOfData')
                        file_group_record['dateAddedToDynamo'] = get_iso_date_as_string()
                        file_group_record = add_file_group_keys(file_group_record)
                        batch.put_item(Item=file_group_record)
                        collection_list.append(file_group_record)
                        object_group_ids.append(object_group_id)
                    image_group_id = my_json.get('imageGroupId')
                    if image_group_id and image_group_id not in image_group_ids:
                        image_group_record = {'imageGroupId': image_group_id}
                        image_group_record['storageSystem'] = my_json.get('storageSystem')
                        image_group_record['typeOfData'] = my_json.get('typeOfData')
                        image_group_record['dateAddedToDynamo'] = get_iso_date_as_string()
                        image_group_record = add_image_group_keys(image_group_record)
                        batch.put_item(Item=image_group_record)
                        collection_list.append(image_group_record)
                        image_group_ids.append(image_group_id)
                    media_group_id = my_json.get('mediaGroupId')
                    if media_group_id and media_group_id not in media_group_ids:
                        media_group_record = {'mediaGroupId': media_group_id}
                        media_group_record['storageSystem'] = my_json.get('storageSystem')
                        media_group_record['typeOfData'] = my_json.get('typeOfData')
                        media_group_record['dateAddedToDynamo'] = get_iso_date_as_string()
                        media_group_record = add_media_group_keys(media_group_record)
                        batch.put_item(Item=media_group_record)
                        collection_list.append(media_group_record)
                        media_group_ids.append(media_group_id)
        return collection_list

    def _cache_s3_call(self, file_name: str, objects: dict):
        """ Save json file locally """
        with open(file_name, 'w') as outfile:
            json.dump(objects, outfile, default=json_serial, sort_keys=True, indent=2)

    def _crawl_available_files_from_s3_or_cache(self, bucket: str, force_use_s3: bool = False) -> dict:
        """ Find all related files, whether from querying S3 or loading from a local json file. """
        cache_file = os.path.join(self.directory, 'crawl_available_files_cache.json')
        if force_use_s3 or (not self.config.get("test", False) and not self.config.get('local', False)):
            objects = crawl_available_files(self.config, bucket)
            if self.config.get('local', False) or self.config.get('forceSaveCrawlAvailableFiles', False):
                self._cache_s3_call(cache_file, objects)
            return objects
        elif os.path.exists(cache_file):
            with io.open(cache_file, 'r', encoding='utf-8') as json_file:
                return json.load(json_file)
        else:
            return {}
