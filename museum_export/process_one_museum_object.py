# process_one_museum_object.py
""" Module to do processing for one museum object """
import os
import time
import json
from clean_up_content import CleanUpContent
from add_image_records_as_child_items import AddImageRecordsAsChildItems
from convert_json_to_csv import ConvertJsonToCsv
from dependencies.pipelineutilities.validate_json import validate_json, get_nd_json_schema
from dependencies.pipelineutilities.s3_helpers import write_s3_file
from dependencies.sentry_sdk import capture_message, push_scope


class ProcessOneMuseumObject():
    def __init__(self, config: dict, image_files: dict, start_time: str):
        self.config = config
        self.image_files = image_files
        self.start_time = start_time
        self.save_despite_missing_fields = False

    def process_object(self, museum_object: dict):
        """ For each object, check for missing fields.  If there are none,
            save information as CSV to S3, and delete the local copy. """
        object_id = museum_object['uniqueIdentifier']
        missing_fields = self._test_for_missing_fields(object_id,
                                                       museum_object,
                                                       self.config['museum-required-fields'])
        print("Missing fields = ", missing_fields)
        if missing_fields == "" or self.save_despite_missing_fields:
            if not validate_museum_json(museum_object):
                print("Validation Error validating ", object_id)

            print("Museum identifier = ", object_id, int(time.time() - self.start_time), 'seconds.')
            clean_up_content_class = CleanUpContent(self.config, self.image_files)
            cleaned_up_object = clean_up_content_class.clean_up_content(museum_object)
            # cleaned_up_object = CleanUpContent(museum_object, self.image_files, self.config).cleaned_up_content
            if not validate_nd_json(cleaned_up_object):
                print("Validation Error validating modified object", object_id)
            self._save_json_to_s3(self.config['process-bucket'], cleaned_up_object, object_id)

#TODO: remove next line
            with open("cleaned_up_object.json", 'w') as f:
                json.dump(cleaned_up_object, f, indent=4)

            csv_file_name = object_id + '.csv'
            convert_json_to_csv_class = ConvertJsonToCsv(self.config["csv-field-names"])
            csv_string = convert_json_to_csv_class.convert_json_to_csv(cleaned_up_object)
            s3_csv_file_name = os.path.join(self.config['process-bucket-csv-basepath'], csv_file_name)
            write_s3_file(self.config['process-bucket'], s3_csv_file_name, csv_string)
            # self.write_csv_locally(csv_file_name, csv_string)
        return missing_fields

    def _test_for_missing_fields(self, object_id: dict, json_object: dict, required_fields: dict) -> str:
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

    def _log_missing_field(self, object_id: str, missing_fields: str):
        """ Log missing field information to sentry """
        with push_scope() as scope:
            scope.set_tag('repository', 'museum')
            scope.set_tag('problem', 'missing_field')
            scope.level = 'warning'
            capture_message(object_id + ' is missing the follwing required field(s): \n' + missing_fields)

    def _save_json_to_s3(self, s3_bucket_name: str, json_object: dict, json_object_id: str) -> bool:
        fully_qualified_file_name = os.path.join("json/" + json_object_id, json_object_id + '.json')
        try:
            write_s3_file(s3_bucket_name, fully_qualified_file_name, json.dumps(json_object))
            results = True
        except Exception:
            results = False
        return results


def write_csv_locally(csv_file_name: str, csv_string: str):
    fully_qualified_file_name = os.path.join("./after_changes", csv_file_name)
    with open(fully_qualified_file_name, 'w') as f:
        f.write(csv_string)
    return


def validate_museum_json(json_to_test: dict) -> bool:
    schema_to_use = get_museum_json_schema()
    valid_json_flag = validate_json(json_to_test, schema_to_use, True)
    return valid_json_flag


def get_museum_json_schema() -> dict:
    schema_to_use = get_nd_json_schema()
    with open('./museum_specific_schema_fields.json') as f:
        museum_specific_schema_fields = json.load(f)
    schema_to_use["properties"].update(museum_specific_schema_fields["properties"])
    return schema_to_use


def validate_nd_json(json_to_test: dict) -> bool:
    valid_json_flag = False
    schema_to_use = get_nd_json_schema()
    valid_json_flag = validate_json(json_to_test, schema_to_use, True)
    return valid_json_flag
