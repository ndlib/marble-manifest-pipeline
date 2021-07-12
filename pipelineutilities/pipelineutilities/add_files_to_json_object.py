import os
from pathlib import Path
from search_files import id_from_url, crawl_available_files, is_media_file  # noqa: #402
from urllib import parse


def _fix_file_metadata_not_on_s3(file_item: dict, parent_unique_identifier: str, media_file_extensions: list) -> dict:
    """ filePath is likely URL where to find file.  change to sourceUri (or sourceFilePath if not URL).
        Change filePath to be destination where this file should be placed (treePath plus filename)
        Add title and description to be filename if they don't exist.
        Set objectFileGroupId (ImageGroupId) or mediaGroupId to parent_unique_identifier
        Set id to be same as new filePath """
    file_path = file_item.get('filePath')
    file_name = os.path.basename(file_path)
    if file_path.startswith('http'):
        source_uri = file_path.replace('http://archives.nd.edu', 'https://archives.nd.edu')
        file_item['sourceUri'] = source_uri
        file_item['storageSystem'] = 'Uri'
        file_item['sourceType'] = 'Uri'
        file_item['typeOfData'] = 'Uri'
        if file_item.get('treePath'):
            file_item['filePath'] = os.path.join(file_item.get('treePath'), file_name)
        else:
            file_path = parse.urlparse(source_uri).path
            if file_path.startswith('/'):
                file_path = file_path[1:]
            file_item['filePath'] = file_path
    else:
        file_item['sourceFilePath'] = file_path
    file_item['title'] = file_item.get('title', file_name)
    file_item['description'] = file_item.get('description', file_name)
    object_file_group_id = id_from_url(file_path)
    if not object_file_group_id:
        object_file_group_id = parent_unique_identifier
    if is_media_file(media_file_extensions, file_name):
        file_item['mediaGroupId'] = object_file_group_id
    else:
        file_item['objectFileGroupId'] = object_file_group_id
        file_item['imageGroupId'] = object_file_group_id
    file_item['id'] = file_item.get('filePath')
    return file_item


def change_file_extensions_to_tif(each_file_dict: dict, file_extensions_to_protect_from_changing_to_tif: list) -> dict:
    """ Change all file extensions to tif except those defined to be protected."""
    for node_name in ['id', 'filePath', 'description', 'title']:
        if node_name in each_file_dict:
            node_value = each_file_dict[node_name]
            file_extension = Path(node_value).suffix
            if file_extension and '.' in file_extension and file_extension.lower() not in file_extensions_to_protect_from_changing_to_tif:
                each_file_dict[node_name] = node_value.replace(file_extension, '.tif')
    return each_file_dict
