import os
from additional_functions import get_seed_nodes_json
from datetime import date
import re


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
    elif external_process_name == 'define_digital_access':
        return_value = _define_digital_access(parameters_json)
    elif external_process_name == 'format_related_ids':
        if 'related_ids' in parameters_json:
            return_value = _format_related_ids(parameters_json["related_ids"])
    elif external_process_name == 'format_subject_uri':
        if 'authFileNumber' in parameters_json:
            return_value = _format_subject_uri(parameters_json["authFileNumber"])
    return return_value


def _define_digital_access(parameters_json: dict) -> str:
    copyrightStatus = parameters_json.get('copyrightStatus', '')
    # copyrightStatement = parameters_json.get('copyrightStatement', '')
    """ If the copyrightStatus contains the word "public", allow Regular access, otherwise Restricted."""
    # return_value = "Restricted"
    return_value = "Regular"  # Until aleph content has copyrightStatus defined, we need to default to Regular access.
    # We will need to define appropriate logic once copyrightStatus is populated.
    if "public" in copyrightStatus.lower():
        return_value = "Regular"
    elif "copyright" in copyrightStatus.lower():
        return_value = "Restricted"
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


def _format_related_ids(value_found: list) -> list:
    """ Return formatted related ids.
    They come in the form: "https://onesearch.library.nd.edu/primo-explore/search?query=any,contains,ndu_aleph001586302&tab=nd_campus&search_scope=nd_campus&vid=NDU" """
    results = []
    regex = r'ndu_aleph[0-9]*'
    sequence = 0
    for value in value_found:
        found_list = re.findall(regex, value)
        if len(found_list) > 0:
            sequence += 1
            node = {"id": found_list[0].replace('ndu_aleph', ''), "sequence": sequence}
            results.append(node)
    return results


def _format_subject_uri(value_found: str) -> str:
    """ Return formatted uri or empty string.
    They come in the form: "sh 95001476 "
    We will return the form: "https://id.loc.gov/authorities/subjects/sh95001476.html" """
    results = ''
    regex = r'sh [0-9]*'
    if re.findall(regex, value_found):
        results = 'https://id.loc.gov/authorities/subjects/' + value_found.replace(' ', '') + '.html'
    return results
