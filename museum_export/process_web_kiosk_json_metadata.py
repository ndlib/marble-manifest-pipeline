# process_web_kiosk_json_metadata.py
""" This routine reads a potentially huge single JSON metadata output file from Web Kiosk.
    Individual json files are created, one per object.
    These individual json files are saved locally by object name.
    They are then uploaded to a Google Team Drive, and deleted locally. """

import json
from datetime import datetime, timedelta
import boto3
import os
from dependencies.sentry_sdk import capture_message, push_scope, capture_exception
from dependencies.pipelineutilities.google_utilities import execute_google_query
from dependencies.pipelineutilities.s3_helpers import write_s3_file
import dependencies.requests
from output_csv import OutputCsv


class processWebKioskJsonMetadata():
    """ This class reads a potentially huge single JSON metadata output file from Web Kiosk.
        Individual json files are created, one per object.
        These individual json files are saved locally by object name.
        They are then uploaded to a Google Team Drive, and deleted locally. """
    def __init__(self, config, google_connection, event):
        self.config = config
        self.event = event
        self.folder_name = "/tmp"
        self.file_name = 'web_kiosk_composite_metadata.json'
        self.composite_json = {}
        s3 = boto3.resource('s3')
        self.bucket = s3.Bucket(self.config['process-bucket'])
        self.google_connection = google_connection
        self.image_files = {}
        self.delete_local_copy = True
        self.save_despite_missing_fields = False

    def get_composite_json_metadata(self, mode):
        """ Build URL, call URL, save resulting output to disk """
        url = self._get_embark_metadata_url(mode)
        self.composite_json = self._get_metadata_given_url(url)
        if self.composite_json:
            fully_qualified_file_name = os.path.join(self.folder_name, self.file_name)
            with open(fully_qualified_file_name, 'w') as f:
                json.dump(self.composite_json, f)
            self._get_image_file_info()
        return self.composite_json

    def _get_image_file_info(self):
        """ Get a list of files which we need to find on Google drive """
        image_files_list = []
        if 'objects' in self.composite_json:
            for object in self.composite_json['objects']:
                if 'digitalAssets' in object:
                    for digital_asset in object['digitalAssets']:
                        image_files_list.append(digital_asset['fileDescription'])
        self._find_images_in_google_drive(image_files_list)
        return image_files_list

    def _find_images_in_google_drive(self, image_files_list):
        """ Go find the list of files from Google drive """
        if len(image_files_list) > 0:
            query_string = "trashed = False and mimeType contains 'image' and ("
            first_pass = True
            for image_file_name in image_files_list:
                if not first_pass:
                    query_string += " or "
                query_string += " name = '" + image_file_name + "'"
                first_pass = False
            query_string += ")"
            drive_id = self.config['museum-google-drive-id']
            results = execute_google_query(self.google_connection, drive_id, query_string)
            for record in results:
                self.image_files[record['name']] = record
        return self.image_files

    def process_composite_json_metadata(self, running_unit_tests=False):
        """ Split big composite metadata file into individual small metadata files """
        objects_processed = 0
        accumulated_missing_fields = ''
        if 'objects' in self.composite_json:
            for object in self.composite_json['objects']:
                if 'uniqueIdentifier' in object:
                    missing_fields = self._process_one_json_object(object)
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

    def _process_one_json_object(self, object):
        """ For each object, check for missing fields.  If there are none,
            save information as CSV to S3, and delete the local copy. """
        object_id = object['uniqueIdentifier']
        print("Processing JSON: ", object_id)
        self._augment_additional_fields(object)
        missing_fields = self._test_for_missing_fields(object_id,
                                                       object,
                                                       self.config['museum-required-fields'])
        if missing_fields == "" or self.save_despite_missing_fields:
            csv_file_name = object_id + '.csv'
            output_csv_class = OutputCsv(self.config["csv-field-names"])
            output_csv_class.write_csv_row(object)
            sequence = 0
            if 'digitalAssets' in object:
                object['digitalAssets'] = json.loads(object['digitalAssets'])  # starting 3/18/20, this became a string, where it should be a json object
                for digital_asset in object['digitalAssets']:
                    if isinstance(digital_asset, dict):
                        self._write_file_csv_record(object, digital_asset, sequence, output_csv_class)
                        sequence += 1
                    else:
                        print("not a dict")
            s3_file_name = os.path.join(self.config['process-bucket-csv-basepath'], csv_file_name)
            write_s3_file(self.config['process-bucket'], s3_file_name, output_csv_class.return_csv_value())
        return missing_fields

    def _augment_additional_fields(self, object):
        self._define_creator(object)
        self._define_worktype(object)
        if 'modifiedDate' in object:
            object['modifiedDate'] = datetime.strptime(object['modifiedDate'], '%m/%d/%Y %H:%M:%S').isoformat() + 'Z'

    def _define_creator(self, object):
        if "artists" in object:
            for artist in object["artists"]:
                role = artist.get("role", "")
                if role == "Primary":
                    object["creator"] = artist.get("fullName", "")
                    break

    def _define_worktype(self, object):
        classifiction = object.get("classification", "")
        if classifiction == "Decorative Arts, Craft, and Design":
            object['workType'] = classifiction
        del object['classification']

    def _write_file_csv_record(self, object, digital_asset, sequence, output_csv_class):
        """ Write file-related information in the CSV file for each image file found for this object. """
        each_file_dict = {}
        each_file_dict['collectionId'] = object['collectionId']
        each_file_dict['parentId'] = object['id']
        each_file_dict['sourceSystem'] = object['sourceSystem']
        each_file_dict['repository'] = object['repository']
        file_name = digital_asset.get("fileDescription", "")
        each_file_dict['id'] = file_name
        if file_name in self.image_files:
            google_image_info = self.image_files[file_name]
            each_file_dict['thumbnail'] = (sequence == 0)
            each_file_dict['level'] = 'file'
            each_file_dict['description'] = file_name
            each_file_dict['fileInfo'] = google_image_info
            each_file_dict['md5Checksum'] = google_image_info['md5Checksum']
            each_file_dict['filePath'] = 'https://drive.google.com/a/nd.edu/file/d/' + google_image_info['id'] + '/view'  # noqa: E501
            each_file_dict['sequence'] = sequence
            each_file_dict['title'] = file_name
            each_file_dict['fileId'] = google_image_info['id']
            each_file_dict['modifiedDate'] = google_image_info['modifiedTime']
            each_file_dict['mimeType'] = google_image_info['mimeType']
            output_csv_class.write_csv_row(each_file_dict)
        return

    def _get_metadata_given_url(self, url):
        """ Return json from URL."""
        json_response = {}
        try:
            json_response = json.loads(dependencies.requests.get(url).text)
        except ConnectionRefusedError:
            capture_exception('Connection refused on url ' + url)
        except:  # noqa E722 - intentionally ignore warning about bare except
            capture_exception('Error caught trying to process url ' + url)
        return json_response

    def _get_embark_metadata_url(self, mode):
        """ Get url for retrieving museum metadata """
        base_url = self.config['museum-server-base-url'] \
            + "/results.html?layout=marble&format=json&maximumrecords=-1&recordType=objects_1"
        if mode == 'full':
            url = base_url + "&query=_ID=ALL"
        else:  # incremental
            recent_past = datetime.utcnow() - timedelta(hours=self.config['hoursThreshold'])
            recent_past_string = recent_past.strftime('%m/%d/%Y')
            url = base_url + "&query=mod_date%3E%22" + recent_past_string + "%22"
        return(url)

    def _test_for_missing_fields(self, object_id, json_object, required_fields):
        """ Test for missing required fields """
        missing_fields = ''
        for preferred_name, json_path in required_fields.items():
            try:
                value = json_object[json_path]
            except KeyError:
                value = None
            if value == '' or value is None:
                missing_fields += preferred_name + ' - at json path location ' + json_path + '\n'
        if missing_fields > '':
            self._log_missing_field(object_id, missing_fields)
            return(object_id + ' is missing the follwing required field(s): \n' + missing_fields + '\n')
        return(missing_fields)

    def _log_missing_field(self, object_id, missing_fields):
        """ Log missing field information to sentry """
        with push_scope() as scope:
            scope.set_tag('repository', 'museum')
            scope.set_tag('problem', 'missing_field')
            scope.level = 'warning'
            capture_message(object_id + ' is missing the follwing required field(s): \n' + missing_fields)


def delete_file(folder_name, file_name):
    """ Delete temparary intermediate file """
    full_path_file_name = os.path.join(folder_name, file_name)
    try:
        os.remove(full_path_file_name)
    except FileNotFoundError:
        pass
