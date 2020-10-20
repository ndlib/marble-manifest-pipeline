""" Standard json Helpers """

import re
from validate_json import validate_standard_json
from add_paths_to_json_object import AddPathsToJsonObject
from expand_subject_terms import expand_subject_terms_recursive
from fix_creators_in_json_object import FixCreatorsInJsonObject
from load_language_codes import load_language_codes
from report_missing_fields import ReportMissingFields
from get_size_of_images import get_size_of_images
from search_files import id_from_url


class StandardJsonHelpers():
    """ Standard Json Helpers Class """
    def __init__(self, config: dict):
        self.config = config

    def enhance_standard_json(self, standard_json: dict) -> dict:
        """ Enhance various standard json nodes """
        add_paths_to_json_object_class = AddPathsToJsonObject(self.config)
        fix_creators_in_json_object_class = FixCreatorsInJsonObject(self.config)
        report_missing_fields_class = ReportMissingFields(self.config)
        standard_json = add_paths_to_json_object_class.add_paths(standard_json)
        standard_json = fix_creators_in_json_object_class.fix_creators(standard_json)
        standard_json = expand_subject_terms_recursive(standard_json)
        standard_json = _clean_up_standard_json_recursive(standard_json)
        standard_json = get_size_of_images(standard_json)
        standard_json = _add_objectFileGroupId(standard_json)
        standard_json = _add_sequence(standard_json)
        if not validate_standard_json(standard_json):
            standard_json = {}
        else:
            report_missing_fields_class.process_missing_fields(standard_json, True)
        return standard_json


def _clean_up_standard_json_recursive(standard_json: dict) -> dict:
    """ Recursively clean up standard_json strings """
    standard_json = _clean_up_standard_json_strings(standard_json)
    for item in standard_json.get('items', []):
        item = _clean_up_standard_json_recursive(item)
    return standard_json


def _clean_up_standard_json_strings(standard_json: dict) -> dict:
    """ clean up various strings, recursively """
    if 'description' in standard_json:
        standard_json['description'] = _remove_brackets(standard_json['description'])
    if 'languages' in standard_json:
        standard_json['languages'] = _add_language_display(standard_json['languages'])
    if 'publisher' in standard_json:
        standard_json['publishers'] = _add_publishers_node(standard_json['publisher'])
        standard_json.pop('publisher')
    lists_to_clean = ['creators', 'contributors', 'subjects', 'publishers', 'collections']
    for list_name in lists_to_clean:
        for individual_node in standard_json.get(list_name, []):
            if individual_node.get('display', ''):
                individual_node['display'] = _remove_trailing_punctuation(individual_node['display'])
    if 'title' in standard_json:
        standard_json['title'] = _remove_trailing_slash(standard_json['title'])
    return standard_json


def _remove_brackets(string_value: str) -> dict:
    """ Strip [] from description """
    regex = r'\[|\]'
    return re.sub(regex, '', string_value).strip()


def _remove_trailing_slash(string_value: str) -> str:
    """ Strip trailing slash (/) at end of string"""
    regex = r'[/]$'
    return re.sub(regex, '', string_value).strip()


def _remove_trailing_punctuation(string_value: str) -> str:
    """ Strip pipe | from anywhere in string, and strip trailing single character .,/:;"""
    regex = r'[|]|[.,/:;]$'
    return re.sub(regex, '', string_value).strip()


def _add_language_display(languages_list: list) -> list:
    new_languages_list = []
    language_codes_list = _load_language_codes()
    for language in languages_list:
        if isinstance(language, str):
            for node in language_codes_list:
                if language.lower() in (node['English'].lower(), node['alpha2'], node['alpha3-b']):
                    new_node = {"display": node['English'], "alpha2": node['alpha2'], "alpha3": node['alpha3-b']}
                    new_languages_list.append(new_node)
                    break
        elif isinstance(language, dict):
            if language.get('display', ''):
                new_languages_list.append(language)
    return new_languages_list


def _add_publishers_node(publisher_node: dict) -> dict:
    publishers = []
    new_node = {}
    if publisher_node.get('publisherName', ''):
        display = _remove_trailing_punctuation(publisher_node['publisherName'])
        new_node['publisherName'] = publisher_node['publisherName']  # intentionally leaving this identical to what was supplied
        if publisher_node.get('publisherLocation', ''):
            display += ', ' + _remove_trailing_punctuation(publisher_node['publisherLocation'])
            new_node['publisherLocation'] = publisher_node['publisherLocation']  # intentionally leaving this identical to what was supplied
        new_node['display'] = display
    elif publisher_node.get('publisherLocation', ''):
        new_node['display'] = _remove_trailing_punctuation(publisher_node['publisherLocation'])
        new_node['publisherLocation'] = publisher_node['publisherLocation']  # intentionally leaving this identical to what was supplied
    if new_node:
        publishers.append(new_node)
    return publishers


def _load_language_codes() -> list:
    """ This list of language codes originated here:
    https://pkgstore.datahub.io/core/language-codes/language-codes-3b2_json/data/3d37ea0e5aa45a469879af23cb9b83be/language-codes-3b2_json.json """
    return load_language_codes()


def _add_objectFileGroupId(standard_json: dict) -> dict:  # noqa: C901
    level = standard_json.get('level', 'manifest')
    if level == 'manifest':
        # manifests can only have files nested directly under them
        object_file_group_id_found = standard_json.get('objectFileGroupId', '') > ''
        default_file_path_found = standard_json.get('defaultFilePath', '') > ''
        for item in standard_json.get('items', ''):
            if item.get('level', '') == 'file':
                if not object_file_group_id_found:
                    object_file_group_id = _find_object_file_group_id(item)
                    if object_file_group_id:
                        standard_json['objectFileGroupId'] = object_file_group_id
                        object_file_group_id_found = True
                if not default_file_path_found and item.get('thumbnail', False):
                    default_file_path = _find_default_file_path(item)
                    if default_file_path:
                        standard_json['defaultFilePath'] = default_file_path
                        default_file_path_found = True
                if object_file_group_id_found and default_file_path_found:
                    break
    elif level == 'collection':
        # collections can have collections or manifests nested directly under them
        for item in standard_json.get('items', ''):
            item = _add_objectFileGroupId(item)
            if item.get('objectFileGroupId', '') and not standard_json.get('objectFileGroupId', ''):
                standard_json['objectFileGroupId'] = item['objectFileGroupId']
            if item.get('defaultFilePath', '') and not standard_json.get('defaultFilePath', ''):
                standard_json['defaultFilePath'] = item['defaultFilePath']
    return standard_json


def _find_object_file_group_id(item: dict) -> str:
    """ Use cascading logic to find object_file_group_id """
    object_file_group_id = item.get('objectFileGroupId', '')
    if not object_file_group_id:
        print("calling id_from_url passing ", item.get('filePath', ''))
        object_file_group_id = id_from_url(item.get('filePath', ''))
    return object_file_group_id


def _find_default_file_path(item: dict) -> str:
    """Use cascading logic to find file path of the representational default file """
    default_file_path = item.get('filePath', '')
    if not default_file_path:
        default_file_path = item.get('fileId', '')
    return default_file_path


def _add_sequence(standard_json: dict) -> dict:
    """ add sequence to standard.json to make sure we keep items ordered correctly """
    sequence = 0
    for item in standard_json.get('items', []):
        sequence += 1
        if 'sequence' not in item:
            item['sequence'] = sequence
        if 'items' in item:
            item = _add_sequence(item)
    if 'sequence' not in standard_json:
        standard_json['sequence'] = 0
    return standard_json
