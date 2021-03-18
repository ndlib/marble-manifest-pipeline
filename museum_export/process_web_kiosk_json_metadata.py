"""
Process Web Kiosk Json Metadata
This routine reads a potentially huge single JSON metadata output file from Web Kiosk.
    Individual json files are created, one per object.
    These individual json files are saved locally by object name.
    They are then uploaded to a Google Team Drive, and deleted locally.
"""

import boto3
import json
from datetime import datetime, timedelta
import os
import time
import dependencies.requests
from dependencies.sentry_sdk import capture_exception
from process_one_museum_object import ProcessOneMuseumObject
from get_image_info_for_all_objects import GetImageInfoForAllObjects
from pipelineutilities.save_standard_json import save_standard_json
from pipelineutilities.save_standard_json_to_dynamo import SaveStandardJsonToDynamo
from pipelineutilities.standard_json_helpers import StandardJsonHelpers
from dynamo_helpers import add_file_keys, add_file_to_process_keys, add_file_group_keys, get_iso_date_as_string
from pipelineutilities.dynamo_query_functions import get_item_record, get_all_file_to_process_records_by_storage_system
import re
from save_json_to_dynamo import SaveJsonToDynamo
from record_files_needing_processed import FilesNeedingProcessed


class ProcessWebKioskJsonMetadata():
    """ This class reads a potentially huge single JSON metadata output file from Web Kiosk.
        Individual json files are created, one per object.
        These individual json files are saved locally by object name.
        They are then uploaded to a Google Team Drive, and deleted locally. """
    def __init__(self, config: dict, event: dict, time_to_break: datetime):
        self.config = config
        self.event = event
        self.time_to_break = time_to_break
        self.folder_name = "/tmp"
        self.file_name = 'web_kiosk_composite_metadata.json'
        self.save_local_copy = False
        self.delete_local_copy = False
        self.start_time = time.time()
        self.table_name = self.config.get('website-metadata-tablename', '')
        self.save_json_to_dynamo_class = SaveJsonToDynamo(config, self.table_name)
        validate_json_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'dependencies', 'pipelineutilities', 'validate_json.py')
        self.validate_json_modified_date = datetime.fromtimestamp(os.path.getmtime(validate_json_path)).isoformat()
        self.file_to_process_records_in_dynamo = {}
        if not self.config.get('local', True):
            self.file_to_process_records_in_dynamo = get_all_file_to_process_records_by_storage_system(self.table_name, 'Google', 'Museum')
            self.table = boto3.resource('dynamodb').Table(self.table_name)

    def get_composite_json_metadata(self, mode: str) -> dict:
        """ Build URL, call URL, save resulting output to disk """
        if mode == "ids":
            composite_json = self._get_composite_json_for_all_named_ids(mode)
        else:
            url = self._get_embark_metadata_url(mode)
            composite_json = self._get_metadata_given_url(url)
        if self.save_local_copy and composite_json:
            fully_qualified_file_name = os.path.join(self.folder_name, self.file_name)
            with open(fully_qualified_file_name, 'w') as output_file:
                json.dump(composite_json, output_file)
            print("Completed retrieving composite metadata from WebKiosk after", int(time.time() - self.start_time), 'seconds.')
        return composite_json

    def _get_composite_json_for_all_named_ids(self, mode: str) -> dict:
        """ Create a single unified composite_json for all ids in a list """
        composite_json = {}
        id_to_process = ""
        if mode == 'ids':
            while "ids" in self.event and len(self.event["ids"]) > 0:
                id_to_process = self.event["ids"].pop(0)
                # print("id_to_process = ", id_to_process)
                url = self._get_embark_metadata_url(mode, id_to_process)
                this_composite_json = self._get_metadata_given_url(url)
                if "objects" in this_composite_json:
                    if not composite_json:
                        composite_json = this_composite_json
                    else:
                        composite_json["objects"].update(this_composite_json["objects"])
        return composite_json

    def find_images_for_composite_json_metadata(self, composite_json: dict) -> dict:
        """ Find images for all objects """
        image_file_info = {}
        if 'objects' in composite_json:
            objects = composite_json["objects"]
            google_credentials = {}
            if self.config.get("museum-google-credentials", ""):
                google_credentials = json.loads(self.config["museum-google-credentials"])
            drive_id = self.config.get("museum-google-drive-id", "")
            image_file_info = GetImageInfoForAllObjects(objects, google_credentials, drive_id).image_file_info
            print("Completed retrieving Google image file info after", int(time.time() - self.start_time), 'seconds.')
        return image_file_info

    def process_composite_json_metadata(self, composite_json: dict, image_file_info: dict) -> int:
        """ Split big composite metadata file into individual small metadata files """
        objects_processed = 0
        if 'objects' in composite_json:
            objects = composite_json["objects"]
            export_all_files_flag = self.event.get('exportAllFilesFlag', False)
            process_one_museum_object_class = ProcessOneMuseumObject(self.config, image_file_info, self.start_time)
            standard_json_helpers_class = StandardJsonHelpers(self.config)
            save_standard_json_to_dynamo_class = SaveStandardJsonToDynamo(self.config)
            list_of_keys = list(objects.keys())
            list_of_keys.sort()
            for object_key in list_of_keys:
                object_value = objects[object_key]
                if 'uniqueIdentifier' in object_value:
                    save_required_flag = self._save_standard_json_to_dynamo_required(object_value)
                    if save_required_flag and datetime.now() < self.time_to_break:
                        standard_json = process_one_museum_object_class.process_object(object_value)
                        standard_json = standard_json_helpers_class.enhance_standard_json(standard_json)
                        save_standard_json_to_dynamo_class.save_standard_json(standard_json)
                        save_standard_json(self.config, standard_json)
                        self._save_google_image_data_to_dynamo(standard_json, export_all_files_flag)
                        objects_processed += 1
                    else:
                        print('no need to re-process standard json for ', object_value.get('uniqueIdentifier'))
                    objects.pop(object_key, None)
                    if datetime.now() >= self.time_to_break:
                        break
        if self.delete_local_copy:
            delete_file(self.folder_name, self.file_name)
        if not self.event.get("local", False):
            print("Saved to s3: ", os.path.join(self.config['process-bucket'], self.config['process-bucket-data-basepath']))
        return objects_processed

    def _get_metadata_given_url(self, url: str) -> dict:
        """ Return json from URL."""
        json_response = {}
        try:
            json_response = json.loads(dependencies.requests.get(url).text)
        except ConnectionRefusedError as e:
            print('Connection refused in process_web_kiosk_json_metadata/_get_metadata_given_url on url ', url)
            capture_exception(e)
        except Exception as e:  # noqa E722 - intentionally ignore warning about bare except
            print('Error caught in process_web_kiosk_json_metadata/_get_metadata_given_url trying to process url ' + url)
            capture_exception(e)
        return json_response

    def _get_embark_metadata_url(self, mode: str, id_to_process: str = "") -> str:
        """ Get url for retrieving museum metadata
            Note:  each time we update the template, we need to switch between
            updating the marble template and the marble_hash template so the version
            in production doesn't break.  The following line must be updated to
            reflect which template is active. """
        base_url = self.config['museum-server-base-url'] \
            + "/results.html?layout=marble&format=json&maximumrecords=-1&recordType=objects_1"
        if mode == 'full':
            url = base_url + "&query=_ID=ALL"
        elif mode == 'ids':
            url = base_url + "&query=Disp_Access_No=" + id_to_process
        else:  # incremental
            recent_past = datetime.utcnow() - timedelta(hours=self.config["hours-threshold-for-incremental-harvest"])
            recent_past_string = recent_past.strftime('%m/%d/%Y')
            url = base_url + "&query=mod_date%3E%22" + recent_past_string + "%22"
        return url

    def _save_google_image_data_to_dynamo(self, standard_json: dict, export_all_files_flag: bool = False):
        """ Save google image data to dynamo recursively """
        if standard_json.get('level', '') == 'file':
            new_dict = {i: standard_json[i] for i in standard_json if i != 'items'}
            new_dict['objectFileGroupId'] = new_dict['parentId']
            new_dict['storageSystem'] = 'Google'
            new_dict['typeOfData'] = 'Museum'
            new_dict = add_file_keys(new_dict)
            item_id = new_dict.get('id')
            with self.table.batch_writer() as batch:
                batch.put_item(Item=new_dict)
                if export_all_files_flag or item_id not in self.file_to_process_records_in_dynamo or new_dict.get('modifiedDate', '') > self.file_to_process_records_in_dynamo[item_id].get('dateModifiedInDynamo', ''):
                    different_json = dict(new_dict)
                    different_json = add_file_to_process_keys(different_json)
                    batch.put_item(Item=different_json)
                    file_group_record = {'objectFileGroupId': new_dict.get('objectFileGroupId')}
                    file_group_record['storageSystem'] = new_dict.get('storageSystem')
                    file_group_record['typeOfData'] = new_dict.get('typeOfData')
                    file_group_record['dateAddedToDynamo'] = get_iso_date_as_string()
                    file_group_record = add_file_group_keys(file_group_record)
                    batch.put_item(Item=file_group_record)
                    # These next 2 lines will need to be removed once image processing is modified to use AppSync
                    files_needing_processed_class = FilesNeedingProcessed(self.config)
                    files_needing_processed_class.record_files_needing_processed(standard_json, True)
        for item in standard_json.get('items', []):
            self._save_google_image_data_to_dynamo(item, export_all_files_flag)

    def _save_standard_json_to_dynamo_required(self, web_kiosk_json: dict) -> bool:
        """ If there is a manual request to save json to dynamo, then flag save.
            If the root record for this item doesn't already exist in dynamo, then flag save.
            If the record saved in dynamo was generated before the currently generated json, then flag save.
            If validate_json.py has been updated more recently than the dateModifiedInDynamo in the dynamo record, then flag save.
            Otherwise, flag no save needed. """
        if self.event.get('forceSaveStandardJson'):
            return True
        item_id = web_kiosk_json.get('uniqueIdentifier')
        record_from_dynamo = {}
        if not self.config.get('local', True) and self.config.get('website-metadata-tablename', '') and item_id:
            record_from_dynamo = get_item_record(self.config.get('website-metadata-tablename', ''), item_id)
        if not record_from_dynamo:
            print("record not in dynamo")
            return True
        last_modified_date_string_from_dynamo = _get_last_modified_date_from_dynamo(record_from_dynamo)
        if not last_modified_date_string_from_dynamo or web_kiosk_json.get('modifiedDate') > last_modified_date_string_from_dynamo:
            print("last_modified_date_string_from_dynamo = ", last_modified_date_string_from_dynamo, 'web_kiosk_modified_date = ', web_kiosk_json.get('modifiedDate'))
            return True
        if self.validate_json_modified_date > record_from_dynamo.get('dateModifiedInDynamo'):
            # print('validate_json_modified_date =', self.validate_json_modified_date, 'dateModifiedInDynamo', record_from_dynamo.get('dateModifiedInDynamo'))
            return True
        return False


def _get_last_modified_date_from_dynamo(record_from_dynamo: dict) -> str:
    modified_date_from_dynamo = record_from_dynamo.get('modifiedDate')
    if not modified_date_from_dynamo:
        return None
    if not _is_date_in_iso_format(modified_date_from_dynamo):
        return None
    return modified_date_from_dynamo


def _is_date_in_iso_format(date_string: str) -> bool:
    """ return True if it looks like this: 2003-01-02T01:02:03, else False """
    regex = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
    found_list = re.findall(regex, date_string)
    if found_list:
        return True
    return False


def delete_file(folder_name: str, file_name: str):
    """ Delete temparary intermediate file """
    full_path_file_name = os.path.join(folder_name, file_name)
    try:
        os.remove(full_path_file_name)
    except FileNotFoundError:
        pass
