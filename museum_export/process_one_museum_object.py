# process_one_museum_object.py
""" Module to do processing for one museum object """
import time
import json
import copy
import os
from clean_up_content import CleanUpContent
from dependencies.pipelineutilities.validate_json import validate_json, get_standard_json_schema, schema_api_version
from sentry_sdk import capture_message, push_scope

local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class ProcessOneMuseumObject():
    def __init__(self, config: dict, image_files: dict, start_time: str):
        self.config = config
        self.image_files = image_files
        self.start_time = start_time
        self.save_despite_missing_fields = True
        self.api_version = schema_api_version()

    def process_object(self, museum_object: dict) -> dict:
        """ For each object, check for missing fields.  If there are none,
            generate standard json. """
        standard_json = {}
        object_id = museum_object['uniqueIdentifier']
        missing_fields = self._test_for_missing_fields(object_id, museum_object, self.config['museum-required-fields'])
        if missing_fields == "" or self.save_despite_missing_fields:
            if not validate_museum_json(museum_object):
                print("Validation Error validating ", object_id)
            print("Museum identifier = ", object_id, int(time.time() - self.start_time), 'seconds.')
            clean_up_content_class = CleanUpContent(self.config, self.image_files, self.api_version)
            standard_json = clean_up_content_class.clean_up_content(museum_object)
            if not validate_standard_json(standard_json):
                print("Validation Error validating cleaned up content standard_json", object_id)
                standard_json = {}
        return standard_json

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
            missing_fields_notification = object_id + ' is missing the following required field(s): \n' + missing_fields + '\n'
            print("Missing fields = ", missing_fields)
            return missing_fields_notification
        return missing_fields

    def _log_missing_field(self, object_id: str, missing_fields: str):
        """ Log missing field information to sentry """
        with push_scope() as scope:
            scope.set_tag('repository', 'museum')
            scope.set_tag('problem', 'missing_field')
            scope.level = 'warning'
            capture_message(object_id + ' is missing the following required field(s): \n' + missing_fields)


def validate_museum_json(json_to_test: dict) -> bool:
    """ validate json coming from EmbARK before processing it """
    schema_to_use = get_museum_json_schema()
    valid_json_flag = validate_json(json_to_test, schema_to_use, True)
    return valid_json_flag


def get_museum_json_schema() -> dict:
    """ get schema appropriate for checking EmbARK json """
    schema_to_use = copy.deepcopy(get_standard_json_schema())
    with open(local_folder + 'museum_specific_schema_fields.json') as f:
        museum_specific_schema_fields = json.load(f)
    schema_to_use["required"] = ["id"]  # explicitly remove parentId and collectionId, since objects with hidden parents don't have these
    schema_to_use["properties"].update(museum_specific_schema_fields["properties"])
    return schema_to_use


def validate_standard_json(json_to_test: dict) -> bool:
    """ validate fixed json against schema """
    valid_json_flag = False
    schema_to_use = get_standard_json_schema()
    valid_json_flag = validate_json(json_to_test, schema_to_use, True)
    return valid_json_flag
