# process_web_kiosk_json_metadata.py
""" This routine reads a potentially huge single JSON metadata output file from Web Kiosk.
    Individual json files are created, one per object.
    These individual json files are saved locally by object name.
    They are then uploaded to a Google Team Drive, and deleted locally. """

import json
from datetime import datetime, timedelta
import time
import os
from dependencies.sentry_sdk import capture_exception
import dependencies.requests
from process_one_museum_object import ProcessOneMuseumObject
from clean_up_composite_json import CleanUpCompositeJson
from get_image_info_for_all_objects import GetImageInfoForAllObjects


class processWebKioskJsonMetadata():
    """ This class reads a potentially huge single JSON metadata output file from Web Kiosk.
        Individual json files are created, one per object.
        These individual json files are saved locally by object name.
        They are then uploaded to a Google Team Drive, and deleted locally. """
    def __init__(self, config: dict, event: dict):
        self.config = config
        self.event = event
        self.folder_name = "/tmp"
        self.file_name = 'web_kiosk_composite_metadata.json'
        self.save_local_copy = True
        self.delete_local_copy = True
        self.start_time = time.time()

    def get_composite_json_metadata(self, mode: str) -> dict:
        """ Build URL, call URL, save resulting output to disk """
        if mode == "ids":
            composite_json = self._get_composite_json_for_all_named_ids(mode)
        else:
            url = self._get_embark_metadata_url(mode)
            composite_json = self._get_metadata_given_url(url)
        if self.save_local_copy and composite_json:
            fully_qualified_file_name = os.path.join(self.folder_name, self.file_name)
            with open(fully_qualified_file_name, 'w') as f:
                json.dump(composite_json, f)
            print("Completed retrieving composite metadata from WebKiosk after", int(time.time() - self.start_time), 'seconds.')
        return composite_json

    def _get_composite_json_for_all_named_ids(self, mode: str) -> dict:
        """ Create a single unified composite_json for all ids in a list """
        composite_json = {}
        id_to_process = ""
        if mode == 'ids':
            while "ids" in self.event and len(self.event["ids"]) > 0:
                id_to_process = self.event["ids"].pop(0)
                url = self._get_embark_metadata_url(mode, id_to_process)
                this_composite_json = self._get_metadata_given_url(url)
                if "objects" in this_composite_json:
                    if not composite_json:
                        composite_json = this_composite_json
                    else:
                        composite_json["objects"].update(this_composite_json["objects"])
        return composite_json

    def process_composite_json_metadata(self, composite_json: dict, running_unit_tests: bool = False) -> int:
        """ Split big composite metadata file into individual small metadata files """
        objects_processed = 0
        accumulated_missing_fields = ''
        if 'objects' in composite_json:
            objects = composite_json["objects"]
            google_credentials = json.loads(self.config["museum-google-credentials"])
            drive_id = self.config['museum-google-drive-id']
            image_file_info = GetImageInfoForAllObjects(objects, google_credentials, drive_id).image_file_info
            print("Completed retrieving Google image file info after", int(time.time() - self.start_time), 'seconds.')
            composite_json = CleanUpCompositeJson(composite_json).cleaned_up_content
            process_one_museum_object_class = ProcessOneMuseumObject(self.config, image_file_info, self.start_time, self.event)
            for _object_key, object_value in objects.items():
                if 'uniqueIdentifier' in object_value:
                    missing_fields = process_one_museum_object_class.process_object(object_value)
                    objects_processed += 1
                    if missing_fields > '':
                        accumulated_missing_fields += missing_fields
                    if running_unit_tests:
                        break
            if accumulated_missing_fields > '':
                # May consider here _log_missing_field oncd per run instead of once per object.
                # self._log_missing_field("Call to Museum JSON harvest ", accumulated_missing_fields)
                pass
        if self.delete_local_copy:
            delete_file(self.folder_name, self.file_name)
        if not self.event.get("local", False):
            print("Saved to s3: ", os.path.join(self.config['process-bucket'], self.config['process-bucket-csv-basepath']))  # noqa: #501
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
        """ Get url for retrieving museum metadata """
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
        return(url)


def delete_file(folder_name: str, file_name: str):
    """ Delete temparary intermediate file """
    full_path_file_name = os.path.join(folder_name, file_name)
    try:
        os.remove(full_path_file_name)
    except FileNotFoundError:
        pass
