import os
from additional_functions import get_seed_nodes_json
from datetime import date


def perform_additional_processing(json_node: dict, field: dict, schema_api_version: int) -> dict:  # noqa: C901
    """ This lets us call other named functions to do additional processing. """
    return_value = ""
    external_process_name = field.get('externalProcess', '')
    parameters_json = {}
    if 'passLabels' in field:
        parameters_json = get_seed_nodes_json(json_node, field['passLabels'])
    if external_process_name == 'schema_api_version':
        return_value = schema_api_version
    if external_process_name == 'file_created_date':
        return_value = str(date.today())
    elif external_process_name == 'get_repository_name_from_ead_resource':
        if 'resource' in parameters_json:
            return_value = get_repository_name_from_ead_resource(parameters_json['resource'])
    elif external_process_name == 'define_level':
        return_value = 'manifest'
        if 'items' in parameters_json:
            return_value = define_manifest_level(parameters_json['items'])
    elif external_process_name == 'file_name_from_filePath':
        if 'filename' in parameters_json:
            return_value = os.path.basename(parameters_json['filename'])
    elif external_process_name == "format_creators":
        if 'creator' in parameters_json:
            return_value = format_creators(parameters_json["creator"])
    return return_value


def get_repository_name_from_ead_resource(ead_resource: str) -> str:
    """ Note:  ead_resource is of the form: 'oai:und//repositories/3/resources/1569'
        This will return standardized names for each of our ArchivesSpace resources. """
    resource = ead_resource.split('/')
    repository_name_dictionary = {"2": "UNDA", "3": "RARE"}
    repository_name = repository_name_dictionary[resource[3]]
    return repository_name


def define_manifest_level(items: list) -> str:
    """ A collection has manifest items.  If the current node does not
        have manifest items, it is a manifest. (A manifest items can have only file items)"""
    level = "manifest"
    if len(items) > 0:
        for item in items:
            if item.get("level", "") == "manifest":
                level = "collection"
                break
    return level


def format_creators(value_found: str) -> dict:
    """ Return formatted creators node."""
    if not value_found:
        value_found = "unknown"
    results = []
    node = {}
    node["attribution"] = ""
    node["role"] = "Primary"
    node["fullName"] = value_found
    node["display"] = value_found
    results.append(node)
    return results
