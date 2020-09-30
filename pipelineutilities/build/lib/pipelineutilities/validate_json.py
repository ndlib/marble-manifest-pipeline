# validate_json.py
""" Common code to retrieve the standard_json_schema and to perform basic json validation against any json schema"""

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


def validate_standard_json(json_to_test: dict) -> bool:
    """ validate fixed json against standard_json_schema """
    valid_json_flag = validate_json(json_to_test, get_standard_json_schema(), True)
    return valid_json_flag


subject_properties = {
    "authority": {"type": "string"},
    "display": {"type": "string"},
    "term": {"type": "string"},
    "uri": {"type": "string"},
    "description": {"type": "string"},
    "parentTerms": {
        "type": "array",
        "items": {"type": "string"}
    },
    "variants": {
        "type": "array",
        "items": {"type": "string"}
    },
    "broaderTerms": {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "authority": {"type": "string"},
                "display": {"type": "string"},
                "term": {"type": "string"},
                "uri": {"type": "string"},
                "parentTerm": {"type": "string"}
            }
        }
    }
}

subject_definition = {
    "type": "object",
    "properties": subject_properties,
    "required": ["term"],
    "additionalProperties": False
}

subject_json_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "description": "Schema for validating a single subject",
    "id": "subject.schema.json",
    "type": "object",
    "properties": subject_properties,
    "required": ["term"],
    "additionalProperties": False
}

subjects_definition = {
    "type": "array",
    "items": subject_definition,
}

subjects_json_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "description": "Schema for validating subjects",
    "title": "Tester for subjects schema",
    "id": "subjects.schema.json",
    "type": "object",
    "properties": {
        "subjects": subjects_definition
    }
}

standard_json_schema = {
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
        "relatedIds": {
            "description": "This facilitates defining non-parent-child relationships.",
            "type": "array",
            "items": {
                "id": {"type": "string"},
                "sequence": {"type": "string"}
            }
        },
        "level": {"type": "string"},
        "title": {"type": "string"},
        "createdDate": {
            "description": "We want dates, like 'c. 1900' or '2020-04-22'.",
            "type": "string"
        },
        "uniqueIdentifier": {"type": "string"},
        "dimensions": {"type": "string"},
        "languages": {
            "description": "Ultimately, we want languages, like ['english'] or ['english', 'french'].",
            "type": "array",
            "items": {
                "anyOf": [
                    {"type": "string"},
                    {
                        "type": "object",
                        "Properties": {
                            "display": {"type": "string"},
                            "alpha2": {"type": "string"},
                            "alpha3": {"type": "string"}
                        }

                    }
                ]
            }
        },
        "subjects": subjects_definition,
        "copyrightStatus": {"type": "string"},
        "copyrightUrl": {"type": "string"},
        "copyrightStatement": {"type": "string"},
        "linkToSource": {"type": "string"},
        "access": {"type": "string"},
        "physicalAccess": {"type": "string"},
        "digitalAccess": {"enum": ["Regular", "Restricted"]},
        "format": {"type": "string"},
        "dedication": {"type": "string"},
        "description": {"type": "string"},
        "modifiedDate": {"type": "string"},
        "thumbnail": {"type": "boolean"},
        "filePath": {"type": "string"},
        "sequence": {"type": "number"},
        "collectionInformation": {"type": "string"},
        "fileId": {"anyOf": [{"type": "string"}, {"type": "boolean"}]},
        "mimeType": {"type": "string"},
        "workType": {"type": "string"},
        "medium": {"type": "string"},
        "publisher": {
            "type": "object",
            "properties": {
                "publisherName": {"type": "string"},
                "publisherLocation": {"type": "string"}
            },
            "required": ["publisherName"],
            "additionalProperties": False
        },
        "publishers": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "display": {"type": "string"},
                    "publisherName": {"type": "string"},
                    "publisherLocation": {"type": "string"}
                },
                "required": ["display"],
                "additionalProperties": False
            }
        },
        "contributors": {
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
        "collections": {
            "description": "This is a list of names of collections to which this object belongs.",
            "type": "array",
            # "items": {"type": "string"}
            "items": {
                "anyOf": [
                    {"type": "string"},
                    {
                        "type": "object",
                        "properties": {"display": {"type": "string"}}
                    }
                ]
            },
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
        "bendoItem": {"type": "string"},  # added 6/10/2020 to support Curate, and subsequent bendo processing
        "items": {
            "type": "array",
            "items": {"$ref": "#"},
            "default": []
        }
    },
    "required": ["id", "parentId", "collectionId", "apiVersion"],
    "additionalProperties": False
}


def get_standard_json_schema():
    """ Return our nd.json schema """
    return standard_json_schema


def get_subjects_json_schema():
    """ Return subjects (plural) schema """
    return subjects_json_schema


def get_subject_json_schema():
    """ Return subject (singular) schema """
    return subject_json_schema


# python -c 'from validate_json import *; test()'
def test():
    """ test various known cases for schema validation success or failure """
    schema_to_use = get_standard_json_schema()
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
        assert result[index] == validation_results
