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
    "GSI1PK": {"type": "string"},
    "GSI1SK": {"type": "string"},
    "GSI2PK": {"type": "string"},
    "GSI2SK": {"type": "string"},
    "PK": {"type": "string"},
    "SK": {"type": "string"},
    "TYPE": {"type": "string"},
    "authority": {"type": "string"},
    "dateAddedToDynamo": {"type": "string"},
    "dateModifiedInDynamo": {"type": "string"},
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
    "description": "Schema for validating standard.json",
    "title": "Tester for ND.json schema",
    "id": "nd.schema.json",
    "type": "object",
    "properties": {
        "GSI1PK": {"type": "string"},
        "GSI1SK": {"type": "string"},
        "GSI2PK": {"type": "string"},
        "GSI2SK": {"type": "string"},
        "PK": {"type": "string"},
        "SK": {"type": "string"},
        "TYPE": {"type": "string"},
        "access": {"type": "string"},
        "additionalNotes": {"type": "string"},
        "apiVersion": {"type": "integer"},
        "bendoItem": {"type": "string"},
        "childIds": {
            "description": "This facilitates defining non-parent-child relationships.",
            "type": "array",
            "items": {
                "id": {"type": "string"},
                "sequence": {"type": "integer"}
            }
        },
        "collectionId": {"type": "string"},
        "collectionInformation": {"type": "string"},
        "collections": {
            "description": "This is a list of names of collections to which this object belongs.",
            "type": "array",
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
        "contributors": {
            "type": "array",
            "items": {
                "description": "Schema for validating creator - Note:  We need to require display once that is added uniformly.",
                "type": "object",
                "properties": {
                    "alive": {"type": "boolean"},
                    "attribution": {"type": "string"},
                    "display": {"type": "string"},
                    "endDate": {"type": "string"},
                    "human": {"type": "boolean"},
                    "fullName": {"type": "string"},
                    "lifeDates": {"type": "string"},
                    "nationality": {"type": "string"},
                    "role": {"type": "string"},
                    "startDate": {"type": "string"}
                },
                "required": ["fullName"],
                "additionalProperties": False
            }
        },
        "copyrightStatement": {"type": "string"},
        "copyrightStatus": {"type": "string"},
        "copyrightUrl": {"type": "string"},
        "createdDate": {
            "description": "We want dates, like 'c. 1900' or '2020-04-22'.",
            "type": "string"
        },
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
        "creators": {
            "type": "array",
            "items": {
                "description": "Schema for validating creator - Note:  We need to require display once that is added uniformly.",
                "type": "object",
                "properties": {
                    "alive": {"type": "boolean"},
                    "attribution": {"type": "string"},
                    "display": {"type": "string"},
                    "endDate": {"type": "string"},
                    "fullName": {"type": "string"},
                    "human": {"type": "boolean"},
                    "lifeDates": {"type": "string"},
                    "nationality": {"type": "string"},
                    "role": {"type": "string"},
                    "startDate": {"type": "string"}
                },
                "required": ["fullName"],
                "additionalProperties": False
            }
        },
        "dateAddedToDynamo": {"type": "string"},
        "dateModifiedInDynamo": {"type": "string"},
        "dedication": {"type": "string"},
        "defaultFilePath": {"type": "string"},
        "description": {"type": "string"},
        "digitalAccess": {"enum": ["Regular", "Restricted"]},
        "digitizationSource": {"type": "string"},
        "dimensions": {"type": "string"},
        "expireTime": {"type": "integer"},
        "fileCreatedDate": {"type": "string"},
        "fileId": {"anyOf": [{"type": "string"}, {"type": "boolean"}]},
        "filePath": {"type": "string"},
        "format": {"type": "string"},
        "geographicLocations": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "authority": {"type": "string"},
                    "display": {"type": "string"},
                },
                "additionalProperties": False
            }
        },
        "height": {"type": "integer"},
        "hierarchySearchable": {"type": "boolean"},
        "id": {"type": "string"},
        "iiifImageUri": {"type": "string"},
        "iiifResourceId": {"type": "string"},
        "iiifUri": {"type": "string"},  # This will be obsolete once websites transition to using iiifResourceId
        "items": {
            "type": "array",
            "items": {"$ref": "#"},
            "default": []
        },
        "key": {"type": "string"},
        "languages": {
            "description": "Ultimately, we want languages, like ['english'] or ['english', 'french'].",
            "type": "array",
            "items": {
                "anyOf": [
                    {"type": "string"},
                    {
                        "type": "object",
                        "Properties": {
                            "alpha2": {"type": "string"},
                            "alpha3": {"type": "string"},
                            "display": {"type": "string"},
                        }

                    }
                ]
            }
        },
        "level": {"enum": ["collection", "manifest", "file"]},
        "linkToSource": {"type": "string"},
        "md5Checksum": {"type": "string"},
        "mediaResourceId": {"type": "string"},
        "mediaServer": {"type": "string"},
        "medium": {"type": "string"},
        "mimeType": {"type": "string"},
        "modifiedDate": {"type": "string"},
        "objectFileGroupId": {"type": "string"},
        "parentId": {"type": "string"},
        "physicalAccess": {"type": "string"},
        "publisher": {
            "type": "object",
            "properties": {
                "publisherLocation": {"type": "string"},
                "publisherName": {"type": "string"},
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
            },
        },
        "relatedIds": {
            "description": "This facilitates defining non-parent-child relationships.",
            "type": "array",
            "items": {
                "id": {"type": "string"},
                "sequence": {"type": "integer"}
            }
        },
        "repository": {"type": "string"},
        "sequence": {"type": "number"},
        "size": {"type": "integer"},
        "sourceBucketName": {"type": "string"},
        "sourceFilePath": {"type": "string"},
        "sourceSystem": {"type": "string"},
        "sourceType": {"enum": ["Curate", "Museum", "S3", "Uri"]},
        "sourceUri": {"type": "string"},
        "storageSystem": {"enum": ["Curate", "Google", "S3", "Uri"]},
        "subjects": subjects_definition,
        "thumbnail": {"type": "boolean"},
        "title": {"type": "string"},
        "treePath": {"type": "string"},
        "typeOfData": {"enum": ["Curate", "Museum", "RBSC website bucket", "Uri"]},
        "uniqueIdentifier": {"type": "string"},
        "width": {"type": "integer"},
        "workType": {"type": "string"},
    },
    "required": ["id", "parentId", "collectionId", "apiVersion", "fileCreatedDate"],
    "additionalProperties": False,
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
