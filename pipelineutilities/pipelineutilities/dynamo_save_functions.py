""" dynamo_save_functions.py
    This module will save specific types of records to DynamoDB
"""
from save_json_to_dynamo import SaveJsonToDynamo
from dynamo_helpers import add_file_group_keys, add_source_system_keys, add_item_to_harvest_keys, add_file_systems_keys, get_iso_date_as_string, \
    add_parent_override_keys, add_file_to_process_keys, add_website_keys, \
    add_new_subject_term_authority_keys, add_unharvested_subject_term_keys, \
    add_subject_term_keys


def save_source_system_record(dynamo_table_name: str, source_system_name: str, save_only_new_records: bool = True):
    """ Save Source System Name """
    config = {'local': False}
    json_record = {'sourceSystem': source_system_name}
    if not save_only_new_records:
        json_record['dateAddedToDynamo'] = get_iso_date_as_string()
    json_record = add_source_system_keys(json_record)
    save_json_to_dynamo_class = SaveJsonToDynamo(config, dynamo_table_name)
    save_json_to_dynamo_class.save_json_to_dynamo(json_record, save_only_new_records)
    return


def save_file_system_record(dynamo_table_name: str, storage_system: str, type_of_data: str, save_only_new_records: bool = True):
    """ Save File System Name """
    config = {'local': False}
    json_record = {'storageSystem': storage_system}
    json_record['typeOfData'] = type_of_data
    if not save_only_new_records:
        json_record['dateAddedToDynamo'] = get_iso_date_as_string()
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
    if not save_only_new_records:
        json_record['dateAddedToDynamo'] = get_iso_date_as_string()
    json_record = add_file_group_keys(json_record)
    save_json_to_dynamo_class = SaveJsonToDynamo(config, dynamo_table_name)
    save_json_to_dynamo_class.save_json_to_dynamo(json_record, save_only_new_records)
    return


def save_harvest_ids(config: dict, source_system: str, string_list_to_save: list, dynamo_table_name: str, save_only_new_records: bool = True):
    """ Loop through items to harvest, saving each to DynamoDB with appropriate keys """
    save_json_to_dynamo_class = SaveJsonToDynamo(config, dynamo_table_name)
    for harvest_item_id in string_list_to_save:
        json_record = {'sourceSystem': source_system, 'harvestItemId': harvest_item_id}
        if not save_only_new_records:
            json_record['dateAddedToDynamo'] = get_iso_date_as_string()
        json_record = add_item_to_harvest_keys(json_record)
        save_json_to_dynamo_class.save_json_to_dynamo(json_record, save_only_new_records)


def save_parent_override_record(dynamo_table_name: str, item_id: str, parent_id: str, sequence: int = 0, save_only_new_records: bool = True):
    """ Save parent override record to dynamo """
    config = {'local': False}
    json_record = {'id': item_id}
    json_record['parentId'] = parent_id
    json_record['sequence'] = sequence
    json_record = add_parent_override_keys(json_record)
    save_json_to_dynamo_class = SaveJsonToDynamo(config, dynamo_table_name)
    save_json_to_dynamo_class.save_json_to_dynamo(json_record, save_only_new_records)


def save_file_to_process_record(dynamo_table_name: str, json_record: dict, save_only_new_records: bool = True):
    """ Save fileToProcess record to dynamo
        Examples of storageSystem include: S3 and Google and Curate
        Examples of typeOfData include: Museum and 'RBSC website bucket' and Curate"""
    config = {'local': False}
    json_record = add_file_to_process_keys(json_record)
    save_json_to_dynamo_class = SaveJsonToDynamo(config, dynamo_table_name)
    save_json_to_dynamo_class.save_json_to_dynamo(json_record, save_only_new_records)


def save_website_record(dynamo_table_name: str, web_site_name: str, save_only_new_records: bool = True):
    """ Save website record to dynamo given web_site_name """
    config = {'local': False}
    json_record = {'id': web_site_name, 'title': web_site_name}
    json_record = add_website_keys(json_record)
    save_json_to_dynamo_class = SaveJsonToDynamo(config, dynamo_table_name)
    save_json_to_dynamo_class.save_json_to_dynamo(json_record, save_only_new_records)


# def save_website_item_record(dynamo_table_name: str, item_id: str, website_id: str, save_only_new_records: bool = True):
#     """ Save WebsiteItem record to dynamo """
#     config = {'local': False}
#     if item_id and website_id:
#         json_record = {'id': item_id}
#         json_record['websiteId'] = website_id
#         json_record = add_website_item_keys(json_record)
#         save_json_to_dynamo_class = SaveJsonToDynamo(config, dynamo_table_name)
#         save_json_to_dynamo_class.save_json_to_dynamo(json_record, save_only_new_records)


def save_new_subject_term_authority_record(dynamo_table_name: str, authority: str, source_system: str, id: str, save_only_new_records: bool = True):
    """ Save NewSubjectTermAuthority record to dynamo """
    config = {'local': False}
    if authority and source_system:
        json_record = {'authority': authority}
        json_record['id'] = id
        json_record['sourceSystem'] = source_system
        json_record = add_new_subject_term_authority_keys(json_record)
        save_json_to_dynamo_class = SaveJsonToDynamo(config, dynamo_table_name)
        save_json_to_dynamo_class.save_json_to_dynamo(json_record, save_only_new_records)


def save_unharvested_subject_term_record(dynamo_table_name: str, authority: str, source_system: str, term: str, id: str, uri: str, save_only_new_records: bool = True):
    """ Save NewSubjectTermAuthority record to dynamo """
    config = {'local': False}
    if authority and term:
        json_record = {'id': id}
        json_record['authority'] = authority
        json_record['term'] = term
        if uri:
            json_record['uri'] = uri
        if source_system:
            json_record['sourceSystem'] = source_system
        json_record = add_unharvested_subject_term_keys(json_record)
        save_json_to_dynamo_class = SaveJsonToDynamo(config, dynamo_table_name)
        save_json_to_dynamo_class.save_json_to_dynamo(json_record, save_only_new_records)


def save_subject_term_record(dynamo_table_name: str, json_record: dict, saving_expanded_record_flag: bool = False, save_only_new_records: bool = False) -> dict:
    """ Save SubjectTerm record to dynamo
        json_record must include uri and authority """
    config = {'local': False}
    results = {}
    if dynamo_table_name and json_record.get('uri') and json_record.get('authority'):
        json_record = add_subject_term_keys(json_record, saving_expanded_record_flag)
        save_json_to_dynamo_class = SaveJsonToDynamo(config, dynamo_table_name)
        dynamo_results = save_json_to_dynamo_class.save_json_to_dynamo_returning_results(json_record, "ALL_OLD", save_only_new_records)
        if 'Attributes' in dynamo_results:
            results = dynamo_results.get('Attributes')
    return results
