# process_web_kiosk_json_metadata.py
""" This routine reads a potentially huge single JSON metadata output file from Web Kiosk.
    Individual json files are created, one per object.
    These individual json files are saved locally by object name.
    They are then uploaded to a Google Team Drive, and deleted locally. """

import json
from datetime import datetime, timedelta
import time
import os
from find_google_images import FindGoogleImages
from clean_up_content import CleanUpContent
from dependencies.sentry_sdk import capture_message, push_scope, capture_exception
from dependencies.pipelineutilities.s3_helpers import write_s3_file
import dependencies.requests
from dependencies.pipelineutilities.output_csv import OutputCsv


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
        self.google_connection = google_connection
        self.image_files = {}
        self.delete_local_copy = True
        self.save_despite_missing_fields = False
        self.start_time = time.time()

    def get_composite_json_metadata(self, mode):
        """ Build URL, call URL, save resulting output to disk """
        url = self._get_embark_metadata_url(mode)
        composite_json = self._get_metadata_given_url(url)
        if composite_json:
            fully_qualified_file_name = os.path.join(self.folder_name, self.file_name)
            with open(fully_qualified_file_name, 'w') as f:
                json.dump(composite_json, f)
            find_google_images_class = FindGoogleImages(self.config, self.google_connection, self.start_time)
            self.image_files = find_google_images_class.get_image_file_info(composite_json)
        return composite_json

    def process_composite_json_metadata(self, composite_json, running_unit_tests=False):
        """ Split big composite metadata file into individual small metadata files """
        objects_processed = 0
        accumulated_missing_fields = ''
        if 'objects' in composite_json:
            for object in composite_json['objects']:
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
        print("Museum identifier = ", object_id, int(time.time() - self.start_time), 'seconds.')
        object = CleanUpContent(object).cleaned_up_content
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
            self._save_json_to_s3(self.config['process-bucket'], object, object_id)
        return missing_fields

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
        except ConnectionRefusedError as e:
            print('Connection refused in process_web_kiosk_json_metadata/_get_metadata_given_url on url ', url)
            capture_exception(e)
        except Exception as e:  # noqa E722 - intentionally ignore warning about bare except
            print('Error caught in process_web_kiosk_json_metadata/_get_metadata_given_url trying to process url ' + url)
            capture_exception(e)
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

    def _save_json_to_s3(self, s3_bucket_name, json_object, json_object_id):
        fully_qualified_file_name = os.path.join("json/" + json_object_id, json_object_id + '.json')
        try:
            write_s3_file(s3_bucket_name, fully_qualified_file_name, json.dumps(json_object))
            results = True
        except Exception:
            results = False
        return results


def delete_file(folder_name, file_name):
    """ Delete temparary intermediate file """
    full_path_file_name = os.path.join(folder_name, file_name)
    try:
        os.remove(full_path_file_name)
    except FileNotFoundError:
        pass
