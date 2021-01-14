""" dynamo_helpers.py
    This module will hold all DynamoDB specific key information for each entity added here
"""
from save_json_to_dynamo import SaveJsonToDynamo
from datetime import datetime


def add_item_keys(standard_json: dict) -> dict:
    """ Add DynamoDB keys to Item record to be saved
        Required values include: id, sequence, parentId, sourceSystem, title
        Note:  New keys added here need to be added to the validate_json module. """
    standard_json['PK'] = 'ITEM#' + format_key_value(standard_json.get('id'))
    standard_json['SK'] = standard_json['PK']
    standard_json['type'] = 'Item'
    standard_json['GSI1PK'] = 'ITEM#' + format_key_value(standard_json.get('parentId'))
    zero_padded_sequence = format(standard_json.get('sequence', 0), '05d')  # zero-pad sequence numbers so they sort correctly as strings
    standard_json['GSI1SK'] = 'SORT#' + str(zero_padded_sequence) + '#' + format_key_value(standard_json.get('title', ''))
    if standard_json.get('parentId').upper() == 'ROOT':
        standard_json['GSI2PK'] = 'SOURCESYSTEM#' + format_key_value(standard_json.get('sourceSystem'))
        standard_json['GSI2SK'] = 'SORT#' + format_key_value(standard_json.get('title', ''))
    standard_json['dateModifiedInDynamo'] = get_iso_date_as_string()
    return standard_json


def add_file_group_keys(file_group_record: dict) -> dict:
    """ Add DynamoDB keys to File Group record to be saved
        Required values include: objectFileGroupId """
    file_group_record['PK'] = 'FILEGROUP'
    file_group_record['SK'] = 'FILEGROUP#' + format_key_value(file_group_record.get('objectFileGroupId'))
    file_group_record['type'] = 'FileGroup'
    file_group_record['GSI1PK'] = file_group_record['SK']
    file_group_record['GSI1SK'] = '#NAME#' + format_key_value(file_group_record.get('objectFileGroupId'))
    file_group_record['dateModifiedInDynamo'] = get_iso_date_as_string()
    return file_group_record


def add_file_keys(file_record: dict) -> dict:
    """ Add DynamoDB keys to File record to be saved
        Required values include: id, objectFileGroupId, sequence """
    file_record['PK'] = 'FILE'
    file_record['SK'] = 'FILE#' + format_key_value(file_record.get('id'))
    file_record['type'] = 'File'
    file_record['GSI1PK'] = 'FILEGROUP#' + format_key_value(file_record.get('objectFileGroupId'))
    zero_padded_sequence = format(file_record.get('sequence', 0), '05d')  # zero-pad sequence numbers so they sort correctly as strings
    file_record['GSI1SK'] = 'SORT#' + str(zero_padded_sequence)
    file_record['dateModifiedInDynamo'] = get_iso_date_as_string()
    return file_record


def add_source_system_keys(source_system_record: dict) -> dict:
    """ Add DynamoDB keys to Source System record to be saved
        Required values include: sourceSystem """
    source_system_record['PK'] = 'SOURCESYSTEM'
    source_system_record['SK'] = 'SOURCESYSTEM#' + format_key_value(source_system_record.get('sourceSystem'))
    source_system_record['type'] = 'SourceSystem'
    source_system_record['GSI2PK'] = source_system_record['SK']
    source_system_record['GSI2SK'] = source_system_record['SK']
    source_system_record['dateModifiedInDynamo'] = get_iso_date_as_string()
    return source_system_record


def save_source_system_record(source_system_name: str, dynamo_table_name: str):
    """ Save Source System Name """
    config = {'local': False}
    source_system_record = {'sourceSystem': source_system_name}
    source_system_record = add_source_system_keys(source_system_record)
    save_json_to_dynamo_class = SaveJsonToDynamo(config, dynamo_table_name)
    save_json_to_dynamo_class.save_json_to_dynamo(source_system_record)
    return


def format_key_value(key_value: str) -> str:
    """ All of our DynamoDB keys must be upper case, with spaces stripped. """
    return key_value.upper().replace(" ", "")


def get_iso_date_as_string() -> str:
    """ This Returns local time, not UTC time """
    return datetime.now().isoformat()
