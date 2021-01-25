""" dynamo_helpers.py
    This module will hold all DynamoDB specific key information for each entity added here
"""
from save_json_to_dynamo import SaveJsonToDynamo
from datetime import datetime


def add_item_keys(json_record: dict) -> dict:
    """ Add DynamoDB keys to Item record to be saved
        Required values include: id, sequence, parentId, sourceSystem, title
        Note:  New keys added here need to be added to the validate_json module. """
    json_record['PK'] = 'ITEM#' + format_key_value(json_record.get('id'))
    json_record['SK'] = json_record['PK']
    json_record['TYPE'] = 'Item'
    json_record['GSI1PK'] = 'ITEM#' + format_key_value(json_record.get('parentId'))
    zero_padded_sequence = format(json_record.get('sequence', 0), '05d')  # zero-pad sequence numbers so they sort correctly as strings
    json_record['GSI1SK'] = 'SORT#' + str(zero_padded_sequence) + '#' + format_key_value(json_record.get('title', ''))
    if json_record.get('parentId').upper() == 'ROOT':
        json_record['GSI2PK'] = 'SOURCESYSTEM#' + format_key_value(json_record.get('sourceSystem'))
        json_record['GSI2SK'] = 'SORT#' + format_key_value(json_record.get('title', ''))
    json_record['dateModifiedInDynamo'] = get_iso_date_as_string()
    return json_record


def add_file_group_keys(json_record: dict) -> dict:
    """ Add DynamoDB keys to File Group record to be saved
        Required values include: objectFileGroupId, storageSystem, typeOfData """
    json_record['PK'] = 'FILEGROUP'
    json_record['SK'] = 'FILEGROUP#' + format_key_value(json_record.get('objectFileGroupId'))
    json_record['TYPE'] = 'FileGroup'
    json_record['GSI1PK'] = json_record['SK']
    json_record['GSI1SK'] = '#NAME#' + format_key_value(json_record.get('objectFileGroupId'))
    json_record['GSI2PK'] = "FILESYSTEM#" + format_key_value(json_record.get('storageSystem')) + '#' + format_key_value(json_record.get('typeOfData'))
    json_record['GSI2SK'] = json_record['SK']
    json_record['dateAddedToDynamo'] = get_iso_date_as_string()
    json_record['dateModifiedInDynamo'] = get_iso_date_as_string()
    return json_record


def add_file_keys(json_record: dict) -> dict:
    """ Add DynamoDB keys to File record to be saved
        Required values include: id, objectFileGroupId, sequence """
    json_record['PK'] = 'FILE'
    json_record['SK'] = 'FILE#' + format_key_value(json_record.get('id'))
    json_record['TYPE'] = 'File'
    json_record['GSI1PK'] = 'FILEGROUP#' + format_key_value(json_record.get('objectFileGroupId'))
    zero_padded_sequence = format(json_record.get('sequence', 0), '05d')  # zero-pad sequence numbers so they sort correctly as strings
    json_record['GSI1SK'] = 'SORT#' + str(zero_padded_sequence)
    json_record['dateModifiedInDynamo'] = get_iso_date_as_string()
    return json_record


def add_source_system_keys(json_record: dict) -> dict:
    """ Add DynamoDB keys to Source System record to be saved
        Required values include: sourceSystem """
    json_record['PK'] = 'SOURCESYSTEM'
    json_record['SK'] = 'SOURCESYSTEM#' + format_key_value(json_record.get('sourceSystem'))
    json_record['TYPE'] = 'SourceSystem'
    json_record['GSI2PK'] = json_record['SK']
    json_record['GSI2SK'] = json_record['SK']
    json_record['dateModifiedInDynamo'] = get_iso_date_as_string()
    return json_record


def add_item_to_harvest_keys(json_record: dict) -> dict:
    """ Add dynamoDB keys to Item To Harest record to be saved
        Required values include: sourceSystem, harvestItemId """
    json_record['PK'] = 'ITEMTOHARVEST'
    json_record['SK'] = 'SOURCESYSTEM#' + format_key_value(json_record.get('sourceSystem')) + '#' + format_key_value(json_record.get('harvestItemId'))
    json_record['TYPE'] = 'ItemToHarvest'
    json_record['dateAddedToDynamo'] = get_iso_date_as_string()
    json_record['dateModifiedInDynamo'] = json_record['dateAddedToDynamo']
    return json_record


def add_file_systems_keys(json_record: dict) -> dict:
    """ Add dynamoDB keys to File System record to be saved
        Required values include: storageSystem, typeOfData
        Examples of storageSystem include: S3 and Google and Curate
        Examples of typeOfData include: Museum and 'RBSC website bucket' and Curate """
    json_record['PK'] = 'FILESYSTEM'
    json_record['SK'] = 'FILESYSTEM#' + format_key_value(json_record.get('storageSystem')) + '#' + format_key_value(json_record.get('typeOfData'))
    json_record['TYPE'] = 'FileSystem'
    json_record['dateAddedToDynamo'] = get_iso_date_as_string()
    json_record['dateModifiedInDynamo'] = json_record['dateAddedToDynamo']
    return json_record


def save_source_system_record(dynamo_table_name: str, source_system_name: str, save_only_new_records: bool = True):
    """ Save Source System Name """
    config = {'local': False}
    json_record = {'sourceSystem': source_system_name}
    json_record = add_source_system_keys(json_record)
    save_json_to_dynamo_class = SaveJsonToDynamo(config, dynamo_table_name)
    save_json_to_dynamo_class.save_json_to_dynamo(json_record, save_only_new_records)
    return


def save_file_system_record(dynamo_table_name: str, storage_system: str, type_of_data: str, save_only_new_records: bool = True):
    """ Save File System Name """
    config = {'local': False}
    json_record = {'storageSystem': storage_system}
    json_record['typeOfData'] = type_of_data
    json_record = add_file_systems_keys(json_record)
    save_json_to_dynamo_class = SaveJsonToDynamo(config, dynamo_table_name)
    save_json_to_dynamo_class.save_json_to_dynamo(json_record, save_only_new_records)
    return


def save_file_group_record(dynamo_table_name: str, object_file_group_id: str, storage_system: str, type_of_data: str, save_only_new_records: bool = True):
    """ Save File System Name """
    config = {'local': False}
    json_record = {'objectFileGroupId': object_file_group_id}
    json_record['storageSystem'] = storage_system
    json_record['typeOfData'] = type_of_data
    json_record = add_file_group_keys(json_record)
    save_json_to_dynamo_class = SaveJsonToDynamo(config, dynamo_table_name)
    save_json_to_dynamo_class.save_json_to_dynamo(json_record, save_only_new_records)
    return


def save_harvest_ids(config: dict, source_system: str, string_list_to_save: list, dynamo_table_name: str, save_only_new_records: bool = True):
    """ Loop through items to harvest, saving each to DynamoDB with appropriate keys """
    save_json_to_dynamo_class = SaveJsonToDynamo(config, dynamo_table_name)
    for harvest_item_id in string_list_to_save:
        new_record = {'sourceSystem': source_system, 'harvestItemId': harvest_item_id}
        new_record = add_item_to_harvest_keys(new_record)
        save_json_to_dynamo_class.save_json_to_dynamo(new_record, save_only_new_records)


def format_key_value(key_value: str) -> str:
    """ All of our DynamoDB keys must be upper case, with spaces stripped. """
    if isinstance(key_value, str):
        key_value = key_value.upper().replace(" ", "")
    return key_value


def get_iso_date_as_string() -> str:
    """ This Returns local time, not UTC time """
    return datetime.now().isoformat()
