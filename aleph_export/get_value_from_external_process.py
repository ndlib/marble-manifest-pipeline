import os
from datetime import date


def get_value_from_external_process(json_node: dict, field: dict, schema_api_version: int) -> dict:
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
    elif external_process_name == 'define_level':
        return_value = define_manifest_level(parameters_json.get('items', []))
    elif external_process_name == 'file_name_from_filePath':
        if 'filename' in parameters_json:
            return_value = os.path.basename(parameters_json['filename'])
    elif external_process_name == 'define_digital_access':
        return_value = _define_digital_access(parameters_json.get("copyrightStatus", ""))
    elif external_process_name == 'find_latest_modified_date':
        return_value = find_latest_modified_date(parameters_json.get('batchModifiedDate'), parameters_json.get('manuallyModifiedDate'))
    elif external_process_name == 'get_unique_identifier':
        return_value = _get_unique_identifier(parameters_json.get('primaryUniqueIdentifier'), parameters_json.get('secondaryUniqueIdentifier'), parameters_json.get('tertiaryUniqueIdentifier'))
    return return_value


def _get_unique_identifier(primary_unique_identifier: str, secondary_unique_identifier: str, tertiary_unique_identifier: str) -> str:
    """ Return a unique identifier starting with the last possibility """
    if tertiary_unique_identifier:
        return tertiary_unique_identifier
    elif secondary_unique_identifier:
        return secondary_unique_identifier
    return primary_unique_identifier


def _define_digital_access(copyrightStatus: str) -> str:
    """ If the copyrightStatus contains the word "public", allow Regular access, otherwise Restricted."""
    # return_value = "Restricted"
    return_value = "Regular"  # Until aleph content has copyrightStatus defined, we need to default to Regular access.
    # We will need to define appropriate logic once copyrightStatus is populated.
    if "public" in copyrightStatus.lower():
        return_value = "Regular"
    elif "copyright" in copyrightStatus.lower():
        return_value = "Restricted"
    return return_value


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


def get_seed_nodes_json(json_node: dict, seed_nodes_control: dict) -> dict:
    """ We need to seed some json sections for extract_fields.
        This seeds those nodes as needed. """
    seed_json_output = {}
    for node in seed_nodes_control:
        for key, value in node.items():
            if value in json_node:
                seed_json_output[key] = json_node[value]
    return seed_json_output


def find_latest_modified_date(batch_modified_date: str, manually_modified_date: str) -> str:
    """ return which ever modified date is the most recent """
    if batch_modified_date > manually_modified_date:
        return batch_modified_date
    return manually_modified_date
