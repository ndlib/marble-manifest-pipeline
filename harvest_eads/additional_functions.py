""" These functions where removed from create_json_from_xml_path.py
    because the module was getting way too big. """

from nodes_exist_in_json_tree import nodes_exist_in_json_tree


def check_for_inconsistent_dao_image_paths(field_record, json_output):
    """ Identify instances where different images in a given DAO record
        point to different directory paths. """
    if 'checkForInconsistencies' in field_record:
        for field in field_record['checkForInconsistencies']:
            value_found = ""
            for record in json_output:  # each daogrp record
                if value_found != "" and value_found != record[field]:
                    print("discrepancy found between ", value_found, ' and ', record[field])
                value_found = record[field]
    return


def file_name_from_filePath(file_path):
    """ Retrieve just the file name given the complete file path."""
    split_file_path = file_path.split('/')
    return_value = split_file_path[len(split_file_path) - 1]
    return return_value


def define_manifest_level(children):
    """ A collection has children.  If the current node does not
        have children, it is a manifest. """
    level = "manifest"
    if len(children) > 0:
        level = "collection"
    return level


def get_repository_name_from_ead_resource(ead_resource):
    """ Note:  ead_resource is of the form: 'oai:und//repositories/3/resources/1569'
        This will return standardized names for each of our ArchivesSpace resources. """
    resource = ead_resource.split('/')
    repository_name_dictionary = {"2": "UNDA", "3": "RARE"}
    repository_name = repository_name_dictionary[resource[3]]
    return repository_name


def return_None_if_needed(value_found, field):
    """ If a field is not populated, and is optional, return None. """
    optional = get_json_value_as_string(field, 'optional')
    if value_found is not None:
        if value_found == {} and optional:
            value_found = None
    return value_found


def get_seed_nodes_json(json_node, seed_nodes_control):
    """ We need to seed some json sections for extract_fields.
        This seeds those nodes as needed. """
    seed_json_output = {}
    for node in seed_nodes_control:
        for key, value in node.items():
            if value in json_node:
                seed_json_output[key] = json_node[value]
    return seed_json_output


def get_xml_node_value(item, return_attribute_name):
    """ This returns the xml text or attribute as specified
        and returns an empty string if not found."""
    value_found = ""
    if return_attribute_name in item.attrib:
        value_found = item.attrib[return_attribute_name]
    else:
        value_found = item.text
    return value_found


def get_value_from_labels(json_object, field):
    """ Given a json_object and a field definition, return
        the value of the first in a list of field names which exists in the json_object."""
    value = {}
    from_labels = get_json_value_as_string(field, 'fromLabels')
    for label_name in from_labels:
        if label_name in json_object:
            value = json_object[label_name]
            break
    return value


def remove_nodes_from_dictionary(json_object, field):
    """ Remove specific nodes from the dictionary. """
    remove_nodes = get_json_value_as_string(field, 'removeNodes')
    for node_to_remove in remove_nodes:
        json_object.pop(node_to_remove, None)
    return None


def enforce_required_descendants(json_node, required_descendants):
    """ If required_descendants do not exist, set the json_node to None. """
    missing_required_descendants = False
    if required_descendants != "":
        missing_required_descendants = not nodes_exist_in_json_tree(json_node, required_descendants)
    if missing_required_descendants:
        json_node = None
    return json_node


def exclude_if_pattern_matches(exclude_pattern, value_found):
    """ This is used to exclude shtml files from DAO content. """
    if exclude_pattern != "":
        for exclude_string in exclude_pattern:
            if exclude_string in value_found:
                value_found = None
    return value_found


def strip_unwanted_whitespace(value_found):
    """ Several fields have trailing whitespace.  This removes that. """
    if value_found is not None:
        value_found = value_found.rstrip()
        if value_found == "":
            value_found = None
    return value_found


def get_json_value_as_string(json_node, label):
    """ Return the value of a node if it exists, otherwise, return an empty string. """
    value = ""
    if label in json_node:
        value = json_node[label]
    return value
