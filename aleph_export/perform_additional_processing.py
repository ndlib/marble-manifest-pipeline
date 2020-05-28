import os
from datetime import date


def perform_additional_processing(json_node: dict, field: dict, schema_api_version: int) -> dict:
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
        return_value = 'manifest'
        if 'items' in parameters_json:
            return_value = define_manifest_level(parameters_json['items'])
    elif external_process_name == 'file_name_from_filePath':
        if 'filename' in parameters_json:
            return_value = os.path.basename(parameters_json['filename'])
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
