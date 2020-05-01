# validate_json.py
""" Common code to retrieve the nd_json_schema and to perform basic json validation against any json schema"""

from jsonschema import validate, ValidationError
from sentry_sdk import capture_exception


def validate_json(json_to_test, schema_to_use, print_error_message=False):
    """ Validate json against a schema """
    results = False
    try:
        validate(instance=json_to_test, schema=schema_to_use)
        results = True
    except ValidationError as e:
        results = False
        if print_error_message:
            print("Validate_json failed with this error message: ", str(e))
        capture_exception(e)
    return results


def schema_api_version():
    """ Define schema version (to be included in json being validated) """
    return 1


nd_json_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "description": "Schema for validating ND.json",
    "title": "Tester for ND.json schema",
    "id": "nd.schema.json",
    "type": "object",
    "properties": {
        "apiVersion": {"type": "integer"},
        "fileCreatedDate": {"type": "string"},
        "id": {"type": "string"},
        "sourceSystem": {"type": "string"},
        "repository": {"type": "string"},
        "collectionId": {"type": "string"},
        "parentId": {"type": "string"},
        "level": {"type": "string"},
        "title": {"type": "string"},
        "dateCreated": {
            "description": "We want dates, like 'c. 1900' or '2020-04-22'.",
            "type": "string"
        },
        "uniqueIdentifier": {"type": "string"},
        "dimensions": {"type": "string"},
        "languages": {
            "description": "Ultimately, we want languages, like ['english'] or ['english', 'french'].",
            "type": "array",
            "items": {"type": "string"}
        },
        "subjects": {
            "type": "array",
            "items": {
                "description": "Ideally, subjects contain URIs, unfortunately, many do not.",
                "type": "object",
                "properties": {
                    "authority": {"type": "string"},
                    "term": {"type": "string"},
                    "uri": {"type": "string"}
                },
                "required": ["term"],
                "additionalProperties": False
            }
        },
        "copyrightStatus": {"type": "string"},
        "copyrightStatement": {"type": "string"},
        "linkToSource": {"type": "string"},
        "access": {"type": "string"},
        "format": {"type": "string"},
        "dedication": {"type": "string"},
        "description": {"type": "string"},
        "modifiedDate": {"type": "string"},
        "thumbnail": {"type": "boolean"},
        "filePath": {"type": "string"},
        "sequence": {"type": ["number", "string"]},
        "collectionInformation": {"type": "string"},
        "fileId": {"type": "string"},
        "mimeType": {"type": "string"},
        "workType": {"type": "string"},
        "medium": {"type": "string"},
        "creators": {
            "type": "array",
            "items": {
                "description": "Schema for validating creator - Note:  We need to require display once that is added uniformly.",
                "type": "object",
                "properties": {
                    "attribution": {"type": "string"},
                    "display": {"type": "string"},
                    "endDate": {"type": "string"},
                    "fullName": {"type": "string"},
                    "lifeDates": {"type": "string"},
                    "human": {"type": "boolean"},
                    "alive": {"type": "boolean"},
                    "nationality": {"type": "string"},
                    "role": {"type": "string"},
                    "startDate": {"type": "string"}
                },
                "required": ["fullName"],
                "additionalProperties": False
            }
        },
        "md5Checksum": {"type": "string"},
        "creationPlace": {
            "type": "object",
            "description": "Schema for validating creationPlace",
            "properties": {
                "city": {"type": "string"},
                "continent": {"type": "string"},
                "country": {"type": "string"},
                "county": {"type": "string"},
                "historic": {"type": "string"},
                "state": {"type": "string"}
            },
            "additionalProperties": False
        },
        "iiifImageUri": {"type": "string"},
        "iiifImageFilePath": {"type": "string"},
        "iiifUri": {"type": "string"},
        "iiifFilePath": {"type": "string"},
        "metsUri": {"type": "string"},
        "metsFilePath": {"type": "string"},
        "schemaUri": {"type": "string"},
        "schemaPath": {"type": "string"},
        "items": {
            "type": "array",
            "items": {"$ref": "#"},
            "default": []
        }
    },
    "required": ["id", "parentId", "collectionId", "apiVersion", "fileCreatedDate"],
    "additionalProperties": False

}


def get_nd_json_schema():
    """ Return our nd.json schema """
    return(nd_json_schema)


# python -c 'from validate_json import *; test()'
def test(identifier=""):
    """ test various known cases for schema validation success or failure """
    schema_to_use = get_nd_json_schema()
    optional_test_mode_parameter = False
    data = [
        {},
        {"id": "123", "parentId": "root", "collectionId": "123"},
        # # test subjects
        {"id": "123", "parentId": "root", "collectionId": "123", "subjects": ""},
        {"id": "123", "parentId": "root", "collectionId": "123", "subjects": [""]},
        {"id": "123", "parentId": "root", "collectionId": "123", "subjects": [{"term": "abc"}]},
        {"id": "123", "parentId": "root", "collectionId": "123", "subjects": [{"something_else": "abc"}]},
        # test creators
        {"id": "123", "parentId": "root", "collectionId": "123", "creators": ""},
        {"id": "123", "parentId": "root", "collectionId": "123", "creators": [""]},
        {"id": "123", "parentId": "root", "collectionId": "123", "creators": [{"fullName": "name", "display": "show_me"}]}
    ]

    # once we add display as a required field in creators again, the following should return False
    # {"id": "123", "parentId": "root", "collectionId": "123", "creators": [{"fullName": "name"}]},

    result = [False, True, True, False, True, False, True, False, True
              ]
    for index, json_to_test in enumerate(data):
        validation_results = validate_json(json_to_test, schema_to_use, optional_test_mode_parameter)
        if validation_results != result[index]:
            print("Expected ", result[index], " when validating ", json_to_test)
        assert(result[index] == validation_results)
