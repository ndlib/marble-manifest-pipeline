""" dynamo_helpers.py
    This module will hold all DynamoDB specific key information for each entity added here
"""
from datetime import datetime
import os
from pathlib import Path


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


def add_file_keys(json_record: dict, iiif_image_service_uri: str = None) -> dict:
    """ Add DynamoDB keys to File record to be saved
        Required values include: id, objectFileGroupId, sequence """
    json_record['PK'] = 'FILE'
    json_record['SK'] = 'FILE#' + format_key_value(json_record.get('id'))
    json_record['TYPE'] = 'File'
    json_record['GSI1PK'] = 'FILEGROUP#' + format_key_value(json_record.get('objectFileGroupId'))
    zero_padded_sequence = format(json_record.get('sequence', 0), '05d')  # zero-pad sequence numbers so they sort correctly as strings
    json_record['GSI1SK'] = 'SORT#' + str(zero_padded_sequence)
    json_record['dateModifiedInDynamo'] = get_iso_date_as_string()
    json_record = _add_more_file_fields(json_record, iiif_image_service_uri)
    return json_record


def add_image_group_keys(json_record: dict) -> dict:
    """ Add DynamoDB keys to Image Group record to be saved
        Required values include: imageGroupId, storageSystem, typeOfData """
    json_record['PK'] = 'IMAGEGROUP'
    json_record['SK'] = 'IMAGEGROUP#' + format_key_value(json_record.get('imageGroupId'))
    json_record['TYPE'] = 'ImageGroup'
    json_record['GSI1PK'] = json_record['SK']
    json_record['GSI1SK'] = '#NAME#' + format_key_value(json_record.get('imageGroupId'))
    json_record['GSI2PK'] = "FILESYSTEM#" + format_key_value(json_record.get('storageSystem')) + '#' + format_key_value(json_record.get('typeOfData'))
    json_record['GSI2SK'] = json_record['SK']
    json_record['dateAddedToDynamo'] = get_iso_date_as_string()
    json_record['dateModifiedInDynamo'] = get_iso_date_as_string()
    return json_record


def add_image_keys(json_record: dict, iiif_image_service_uri: str = None) -> dict:
    """ Add DynamoDB keys to Image record to be saved
        Required values include: id, imageGroupId, sequence """
    json_record['PK'] = 'IMAGE'
    json_record['SK'] = 'IMAGE#' + format_key_value(json_record.get('id'))
    json_record['TYPE'] = 'Image'
    json_record['GSI1PK'] = 'IMAGEGROUP#' + format_key_value(json_record.get('imageGroupId'))
    zero_padded_sequence = format(json_record.get('sequence', 0), '05d')  # zero-pad sequence numbers so they sort correctly as strings
    json_record['GSI1SK'] = 'SORT#' + str(zero_padded_sequence)
    json_record['dateModifiedInDynamo'] = get_iso_date_as_string()
    json_record = _add_more_file_fields(json_record, iiif_image_service_uri)
    return json_record


def add_media_group_keys(json_record: dict) -> dict:
    """ Add DynamoDB keys to Media Group record to be saved
        Required values include: mediaGroupId, storageSystem, typeOfData """
    json_record['PK'] = 'MEDIAGROUP'
    json_record['SK'] = 'MEDIAGROUP#' + format_key_value(json_record.get('mediaGroupId'))
    json_record['TYPE'] = 'MediaGroup'
    json_record['GSI1PK'] = json_record['SK']
    json_record['GSI1SK'] = '#NAME#' + format_key_value(json_record.get('mediaGroupId'))
    json_record['GSI2PK'] = "FILESYSTEM#" + format_key_value(json_record.get('storageSystem')) + '#' + format_key_value(json_record.get('typeOfData'))
    json_record['GSI2SK'] = json_record['SK']
    json_record['dateAddedToDynamo'] = get_iso_date_as_string()
    json_record['dateModifiedInDynamo'] = get_iso_date_as_string()
    return json_record


def add_media_keys(json_record: dict, media_service_uri: str = None) -> dict:
    """ Add DynamoDB keys to Media record to be saved
        Required values include: id, mediaGroupId, sequence """
    json_record['PK'] = 'MEDIA'
    json_record['SK'] = 'MEDIA#' + format_key_value(json_record.get('id'))
    json_record['TYPE'] = 'Media'
    json_record['GSI1PK'] = 'MEDIAGROUP#' + format_key_value(json_record.get('mediaGroupId'))
    zero_padded_sequence = format(json_record.get('sequence', 0), '05d')  # zero-pad sequence numbers so they sort correctly as strings
    json_record['GSI1SK'] = 'SORT#' + str(zero_padded_sequence)
    json_record['dateModifiedInDynamo'] = get_iso_date_as_string()
    json_record = _add_more_file_fields(json_record, media_service_uri)
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


def add_parent_override_keys(json_record: dict) -> dict:
    """ Add dynamoDB keys to Parent Override record to be saved
        Required values include: id (itemId), parentId """
    json_record['PK'] = 'ITEM#' + format_key_value(json_record.get('id'))
    json_record['SK'] = 'PARENT#' + format_key_value(json_record.get('parentId'))
    json_record['TYPE'] = 'ParentOverride'
    json_record['GSI1PK'] = 'PARENTOVERRIDE'
    json_record['GSI1SK'] = json_record['PK']
    json_record['dateAddedToDynamo'] = get_iso_date_as_string()
    json_record['dateModifiedInDynamo'] = json_record['dateAddedToDynamo']
    return json_record


def add_file_to_process_keys(json_record: dict) -> dict:
    """ Add dynamoDB keys to FileToProcess record to be saved
        Required values include: filePath, storageSystem, typeOfData, dateLastProcessed
        Examples of storageSystem include: S3 and Google and Curate
        Examples of typeOfData include: Museum and 'RBSC website bucket' and Curate"""
    json_record['PK'] = 'FILETOPROCESS'
    json_record['SK'] = 'FILEPATH#' + format_key_value(json_record.get('filePath'))
    json_record['TYPE'] = 'FileToProcess'
    json_record['GSI1PK'] = 'FILETOPROCESS'
    json_record['GSI1SK'] = 'FILESYSTEM#' + format_key_value(json_record.get('storageSystem')) + '#' + format_key_value(json_record.get('typeOfData'))
    json_record['GSI2PK'] = 'FILETOPROCESS'
    json_record['GSI2SK'] = 'DATELASTPROCESSED#' + format_key_value(json_record.get('dateLastProcessed', ''))  # Note this will always be blank at harvest time
    json_record['dateAddedToDynamo'] = get_iso_date_as_string()
    json_record['dateModifiedInDynamo'] = json_record['dateAddedToDynamo']
    return json_record


def add_website_keys(json_record: dict) -> dict:
    """ Add dynamoDB keys to Website record to be saved
        Required values include: id (website name) """
    json_record['PK'] = 'WEBSITE'
    json_record['SK'] = 'WEBSITE#' + format_key_value(json_record.get('id'))
    json_record['TYPE'] = 'WebSite'
    json_record['GSI1PK'] = json_record['SK']
    json_record['GSI1SK'] = json_record['SK']
    json_record['dateAddedToDynamo'] = get_iso_date_as_string()
    json_record['dateModifiedInDynamo'] = json_record['dateAddedToDynamo']
    return json_record


def add_website_item_keys(json_record: dict) -> dict:
    """ Add dynamoDB keys to WebsiteItem record to be saved
        Required values include: websiteId, id (itemId) """
    json_record['PK'] = 'WEBSITE#' + format_key_value(json_record.get('websiteId'))
    json_record['SK'] = 'ITEM#' + format_key_value(json_record.get('id'))
    json_record['TYPE'] = 'WebsiteItem'
    json_record['GSI1PK'] = json_record['PK']
    json_record['GSI1SK'] = 'ADDED#' + get_iso_date_as_string()
    json_record['dateAddedToDynamo'] = get_iso_date_as_string()
    json_record['dateModifiedInDynamo'] = json_record['dateAddedToDynamo']
    json_record['itemId'] = json_record['id']
    return json_record


def add_new_subject_term_authority_keys(json_record: dict) -> dict:
    """ Add dynamoDB keys to NewSubjectTermAuthority record to be saved
        Required values include: authority, sourceSystem """
    json_record['PK'] = 'NEWSUBJECTTERMAUTHORITY'
    json_record['SK'] = 'AUTHORITY#' + format_key_value(json_record.get('authority', ''))
    json_record['TYPE'] = 'NewSubjectTermAuthority'
    json_record['GSI1PK'] = json_record['PK']
    json_record['GSI1SK'] = 'SOURCESYSTEM#' + format_key_value(json_record.get('sourceSystem'))
    json_record['dateAddedToDynamo'] = get_iso_date_as_string()
    json_record['dateModifiedInDynamo'] = json_record['dateAddedToDynamo']
    return json_record


def add_unharvested_subject_term_keys(json_record: dict) -> dict:
    """ Add dynamoDB keys to NewSubjectTermAuthority record to be saved
        Required values include: term, authority, source_system """
    json_record['PK'] = 'UNHARVESTEDSUBJECTTERM'
    json_record['SK'] = 'TERM#' + format_key_value(json_record.get('term'))
    json_record['TYPE'] = 'UnharvestedSubjectTerm'
    json_record['GSI1PK'] = json_record['PK']
    json_record['GSI1SK'] = 'AUTHORITY#' + format_key_value(json_record.get('authority', ''))
    json_record['dateAddedToDynamo'] = get_iso_date_as_string()
    json_record['dateModifiedInDynamo'] = json_record['dateAddedToDynamo']
    return json_record


def add_subject_term_keys(json_record: dict, saving_expanded_record_flag: bool = False) -> dict:
    """ Add dynamoDB keys to SubjectTerm record to be saved
        Required values include: uri, authority """
    json_record['PK'] = 'SUBJECTTERM'
    json_record['SK'] = 'URI#' + format_key_value(json_record.get('uri'))
    json_record['TYPE'] = 'SubjectTerm'
    if 'dateAddedToDynamo' not in json_record:
        json_record['dateAddedToDynamo'] = get_iso_date_as_string()
    if saving_expanded_record_flag and 'dateModifiedInDynamo' not in json_record:
        json_record['dateModifiedInDynamo'] = get_iso_date_as_string()
    json_record['GSI1PK'] = json_record['PK']
    json_record['GSI1SK'] = 'AUTHORITY#' + format_key_value(json_record.get('authority', '')) + '#'
    json_record['GSI2PK'] = json_record['PK']
    json_record['GSI2SK'] = 'LASTHARVESTDATE#'
    if 'dateModifiedInDynamo' in json_record:
        json_record['GSI1SK'] = 'AUTHORITY#' + format_key_value(json_record.get('authority', '')) + '#' + json_record['dateModifiedInDynamo']
        json_record['GSI2SK'] = 'LASTHARVESTDATE#' + json_record['dateModifiedInDynamo']
    return json_record


def add_supplemental_data_keys(json_record: dict) -> dict:
    """ Add dynamoDB keys to SupplementalData record to be saved
        Required values include: [websiteId], id (itemId) """
    json_record['websiteId'] = json_record.get('websiteId', 'All')
    json_record['PK'] = 'ITEM#' + format_key_value(json_record.get('id'))
    json_record['SK'] = 'SUPPLEMENTALDATA#' + format_key_value(json_record.get('websiteId'))
    json_record['TYPE'] = 'SupplementalData'
    json_record['GSI1PK'] = 'SUPPLEMENTALDATA'
    json_record['GSI1SK'] = 'ITEM#' + format_key_value(json_record.get('id'))
    json_record['dateAddedToDynamo'] = get_iso_date_as_string()
    json_record['dateModifiedInDynamo'] = json_record['dateAddedToDynamo']
    return json_record


def format_key_value(key_value: str) -> str:
    """ All of our DynamoDB keys must be upper case, with spaces stripped. """
    if isinstance(key_value, str):
        key_value = key_value.upper().replace(" ", "")
    return key_value


def get_iso_date_as_string() -> str:
    """ This Returns local time, not UTC time """
    return datetime.now().isoformat()


def _add_more_file_fields(json_record: dict, image_or_media_service_uri: str = None) -> dict:
    """ Add mimeType (if absent), add mediaServer and mediaResourceId """
    """ Note: While %2F works for both the iiif_image_service and for the multimedia service, only iiif_image_service requires %2F instead of / """
    file_path = json_record.get('filePath')
    if file_path:
        file_extension = Path(file_path).suffix
        if file_extension and '.' in file_extension and file_extension.lower() in ['.tif']:
            json_record['mimeType'] = json_record.get('mimeType', 'image/tiff')
            file_path_no_extension = os.path.join(Path(file_path).parent, Path(file_path).stem)
            json_record['mediaResourceId'] = file_path_no_extension.replace('/', '%2F')
        elif file_extension and '.' in file_extension and file_extension.lower() in ['.pdf']:
            json_record['mimeType'] = json_record.get('mimeType', 'application/pdf')
            json_record['mediaResourceId'] = file_path.replace('/', '%2F')
        if image_or_media_service_uri:
            json_record['mediaServer'] = image_or_media_service_uri
    return json_record
