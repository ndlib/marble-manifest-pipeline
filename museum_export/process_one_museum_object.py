# process_one_museum_object.py
""" Module to do processing for one museum object """
import os
import time
import json
from clean_up_content import CleanUpContent
from dependencies.pipelineutilities.output_csv import OutputCsv
from dependencies.pipelineutilities.validate_json import validate_json, get_nd_json_schema
from dependencies.pipelineutilities.s3_helpers import write_s3_file
from dependencies.sentry_sdk import capture_message, push_scope, capture_exception


class ProcessOneMuseumObject():
    def __init__(self, config, image_files, start_time):
        self.config = config
        self.image_files = image_files
        self.start_time = start_time
        self.save_despite_missing_fields = False

    def process_object(self, object):
        """ For each object, check for missing fields.  If there are none,
            save information as CSV to S3, and delete the local copy. """
        object_id = object['uniqueIdentifier']
        print("Museum identifier = ", object_id, int(time.time() - self.start_time), 'seconds.')
        object = CleanUpContent(object).cleaned_up_content
        if not validate_nd_json(object):
            print("Validation Error validating ", object_id)
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
            # self.write_csv_locally(csv_file_name, output_csv_class.return_csv_value())
            self._save_json_to_s3(self.config['process-bucket'], object, object_id)
        return missing_fields

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


def write_csv_locally(csv_file_name, csv_string):
    fully_qualified_file_name = os.path.join("./after_changes", csv_file_name)
    with open(fully_qualified_file_name, 'w') as f:
        f.write(csv_string)
    return


def validate_nd_json(json_to_test):
    valid_json_flag = False
    schema_to_use = get_nd_json_schema()
    valid_json_flag = validate_json(json_to_test, schema_to_use, True)
    return valid_json_flag
