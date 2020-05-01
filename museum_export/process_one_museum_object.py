# process_one_museum_object.py
""" Module to do processing for one museum object """
import os
import time
import json
import copy
from clean_up_content import CleanUpContent
from convert_json_to_csv import ConvertJsonToCsv
from dependencies.pipelineutilities.validate_json import validate_json, get_nd_json_schema, schema_api_version
from pipelineutilities.s3_helpers import write_s3_file, write_s3_json
from sentry_sdk import capture_message, push_scope


class ProcessOneMuseumObject():
    def __init__(self, config: dict, image_files: dict, start_time: str):
        self.config = config
        self.image_files = image_files
        self.start_time = start_time
        self.save_despite_missing_fields = True
        self.api_version = schema_api_version()

    def process_object(self, museum_object: dict):
        """ For each object, check for missing fields.  If there are none,
            save information as CSV to S3, and delete the local copy. """
        object_id = museum_object['uniqueIdentifier']
        missing_fields = self._test_for_missing_fields(object_id, museum_object, self.config['museum-required-fields'])
        if missing_fields == "" or self.save_despite_missing_fields:
            if not validate_museum_json(museum_object):
                print("Validation Error validating ", object_id)
            print("Museum identifier = ", object_id, int(time.time() - self.start_time), 'seconds.')
            clean_up_content_class = CleanUpContent(self.config, self.image_files, self.api_version)
            cleaned_up_object = clean_up_content_class.clean_up_content(museum_object)
            if not validate_nd_json(cleaned_up_object):
                print("Validation Error validating modified object", object_id)
            write_s3_json(self.config['process-bucket'], os.path.join("json/", object_id + '.json'), cleaned_up_object)
            # with open("cleaned_up_object.json", 'w') as f:
            #     json.dump(cleaned_up_object, f, indent=4)
            self._save_csv(object_id, cleaned_up_object)
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
            missing_fields_notification = object_id + ' is missing the follwing required field(s): \n' + missing_fields + '\n'
            print("Missing fields = ", missing_fields)
            return missing_fields_notification
        return missing_fields

    def _log_missing_field(self, object_id: str, missing_fields: str):
        """ Log missing field information to sentry """
        with push_scope() as scope:
            scope.set_tag('repository', 'museum')
            scope.set_tag('problem', 'missing_field')
            scope.level = 'warning'
            capture_message(object_id + ' is missing the follwing required field(s): \n' + missing_fields)

    def _save_csv(self, object_id: str, object: dict):
        """ save csv string as a csv file """
        csv_file_name = object_id + '.csv'
        convert_json_to_csv_class = ConvertJsonToCsv(self.config["csv-field-names"])
        csv_string = convert_json_to_csv_class.convert_json_to_csv(object)
        if self.config["local"]:
            write_csv_locally(csv_file_name, csv_string)
        else:
            s3_csv_file_name = os.path.join(self.config['process-bucket-csv-basepath'], csv_file_name)
            write_s3_file(self.config['process-bucket'], s3_csv_file_name, csv_string)


def write_csv_locally(csv_file_name: str, csv_string: str):
    """ save csv locally """
    fully_qualified_file_name = os.path.join("./after_changes", csv_file_name)
    with open(fully_qualified_file_name, 'w') as f:
        f.write(csv_string)
    return


def validate_museum_json(json_to_test: dict) -> bool:
    """ validate json coming from EmbARK before processing it """
    schema_to_use = get_museum_json_schema()
    valid_json_flag = validate_json(json_to_test, schema_to_use, True)
    return valid_json_flag


def get_museum_json_schema() -> dict:
    """ get schema appropriate for checking EmbARK json """
    schema_to_use = copy.deepcopy(get_nd_json_schema())
    with open('./museum_specific_schema_fields.json') as f:
        museum_specific_schema_fields = json.load(f)
    schema_to_use["required"] = ["id"]  # explicitly remove parentId and collectionId, since objects with hidden parents don't have these
    schema_to_use["properties"].update(museum_specific_schema_fields["properties"])
    return schema_to_use


def validate_nd_json(json_to_test: dict) -> bool:
    """ validate fixed json against schema """
    valid_json_flag = False
    schema_to_use = get_nd_json_schema()
    valid_json_flag = validate_json(json_to_test, schema_to_use, True)
    return valid_json_flag
