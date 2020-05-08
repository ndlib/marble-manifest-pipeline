""" These functions where removed from create_json_from_xml_path.py
    because the module was getting way too big. """


def get_json_value_as_string(json_node: dict, label: str) -> str:
    """ Return the value of a node if it exists, otherwise, return an empty string. """
    value = ""
    if label in json_node:
        value = json_node[label]
    return value


def get_seed_nodes_json(json_node: dict, seed_nodes_control: dict) -> dict:
    """ We need to seed some json sections for extract_fields.
        This seeds those nodes as needed. """
    seed_json_output = {}
    for node in seed_nodes_control:
        for key, value in node.items():
            if value in json_node:
                seed_json_output[key] = json_node[value]
    return seed_json_output


def return_None_if_needed(value_found: dict, field: dict) -> dict:
    """ If a field is not populated, and is optional, return None. """
    optional = get_json_value_as_string(field, 'optional')
    if value_found is not None:
        if value_found == {} and optional:
            value_found = None
    return value_found


def get_value_from_labels(json_object: dict, field: dict) -> dict:
    """ Given a json_object and a field definition, return
        the value of the first in a list of field names which exists in the json_object."""
    value = {}
    from_labels = get_json_value_as_string(field, 'fromLabels')
    for label_name in from_labels:
        if label_name in json_object:
            value = json_object[label_name]
            break
    return value


def remove_nodes_from_dictionary(json_object: dict, field: dict) -> None:
    """ Remove specific nodes from the dictionary. """
    remove_nodes = get_json_value_as_string(field, 'removeNodes')
    for node_to_remove in remove_nodes:
        json_object.pop(node_to_remove, None)
    return None


def exclude_if_pattern_matches(exclude_pattern: list, value_found: str) -> str:
    """ This is used to exclude shtml files from DAO content. """
    if exclude_pattern != "":
        for exclude_string in exclude_pattern:
            if exclude_string in value_found:
                value_found = None
                break
    return value_found


def strip_unwanted_whitespace(value_found: str) -> str:
    """ Several fields have trailing whitespace.  This removes that. """
    if value_found is not None:
        value_found = value_found.strip()
        if value_found == "":
            value_found = None
    return value_found


def file_name_from_filePath(file_path: str) -> str:
    """ Retrieve just the file name given the complete file path."""
    split_file_path = file_path.split('/')
    return_value = split_file_path[len(split_file_path) - 1]
    return return_value
