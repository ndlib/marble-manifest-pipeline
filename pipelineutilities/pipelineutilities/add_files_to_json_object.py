import json
import os
from pathlib import Path
from datetime import datetime
from search_files import id_from_url, crawl_available_files, is_media_file  # noqa: #402
from urllib import parse


class AddFilesToJsonObject():
    def __init__(self, config: dict):
        self.config = config
        parent_folder = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + "/"
        if config.get("local", False):
            with open(parent_folder + 'test/hash_of_available_files.json', 'r') as input_source:
                self.hash_of_available_files = json.load(input_source)
        else:
            self.hash_of_available_files = crawl_available_files(self.config, self.config['rbsc-image-bucket'])
            # with open(parent_folder + 'test/hash_of_available_files.json', 'w') as f:
            #     json.dump(self.hash_of_available_files, f, indent=2, default=str)

    def add_files(self, standard_json: dict) -> dict:
        """ recursively go through all of standard_json finding files, and adding additional files """
        if "items" in standard_json:
            for index, item in enumerate(standard_json["items"]):
                if item.get("level", "") in ["manifest", "collection"]:
                    self.add_files(item)
                elif item.get("level", "") == "file":
                    file_path = item.get("filePath", "")
                    if file_path:
                        parent_unique_identifier = standard_json.get('uniqueIdentifier')
                        if not parent_unique_identifier:
                            parent_unique_identifier = standard_json.get('id')
                        self._add_other_files_given_uri(standard_json["items"], index, file_path, parent_unique_identifier)
                        break  # assumption is that we process only the first item to find additional files
        return standard_json

    def _add_other_files_given_uri(self, file_items: list, index: int, file_path: str, parent_unique_identifier: str):
        """ This accepts an image uri, and finds (and appends to the standard_json)
            all related images. """
        each_file_dict = {}
        id_to_find = id_from_url(file_path)
        if id_to_find in self.hash_of_available_files:
            item_id = file_items[index]['id']
            item_description = file_items[index]['description']
            collection_id = file_items[index]["collectionId"]
            source_system = file_items[index]["sourceSystem"]
            repository = file_items[index]["repository"]
            api_version = file_items[index]["apiVersion"]
            file_created_date = file_items[index]["fileCreatedDate"]
            parent_id = file_items[index]["parentId"]
            if 'files' in self.hash_of_available_files[id_to_find]:
                file_items.pop(index)
                sequence = 0
                for obj in self.hash_of_available_files[id_to_find]['files']:
                    each_file_dict = {}
                    if is_media_file(self.config.get('media-file-extensions', []), obj['key']):
                        each_file_dict['mediaGroupId'] = id_to_find
                    else:
                        each_file_dict['objectFileGroupId'] = id_to_find
                        each_file_dict['imageGroupId'] = id_to_find
                    each_file_dict['collectionId'] = collection_id
                    each_file_dict['sourceSystem'] = source_system
                    each_file_dict['repository'] = repository
                    each_file_dict['apiVersion'] = api_version
                    each_file_dict['fileCreatedDate'] = file_created_date
                    each_file_dict['level'] = 'file'
                    each_file_dict['parentId'] = parent_id
                    each_file_dict['id'] = obj['key']  # going forward, I believe we will want id to be the full destination path
                    each_file_dict['filePath'] = obj['key']  # going forward, I believe we will want id to be the full destination path
                    each_file_dict['key'] = str(obj['key'])
                    each_file_dict['thumbnail'] = (each_file_dict['id'] == item_id or os.path.basename(obj['key']) == item_id)
                    each_file_dict['description'] = ""
                    if each_file_dict['id'] == item_id or os.path.basename(obj['key']) == item_id:
                        each_file_dict['description'] = item_description
                    else:
                        each_file_dict['description'] = os.path.basename(obj['key'])
                    each_file_dict['sourceFilePath'] = str(obj['path'])
                    sequence += 1
                    each_file_dict['sequence'] = obj.get('order', sequence)
                    each_file_dict['title'] = str(obj['label'])
                    if obj['lastModified']:
                        each_file_dict['modifiedDate'] = obj['lastModified']
                        if isinstance(each_file_dict['modifiedDate'], str):
                            obj['lastModified'] = obj['lastModified'].replace("+00:00", "").replace("T", " ").replace('Z', '')
                            each_file_dict['modifiedDate'] = datetime.strptime(obj['lastModified'], '%Y-%m-%d %H:%M:%S').isoformat() + '+00:00Z'
                        elif isinstance(each_file_dict['modifiedDate'], datetime):
                            each_file_dict['modifiedDate'] = obj['lastModified'].isoformat() + 'Z'
                    each_file_dict['md5Checksum'] = obj['eTag'].replace("'", "").replace('"', '')  # strip duplicated quotes: {'ETag': '"8b50cfed39b7d8bcb4bd652446fe8adf"'}  # noqa: E501
                    if self._file_exists_in_list(file_items, each_file_dict['id']):
                        self._remove_existing_file_from_list(file_items, each_file_dict['id'])
                    each_file_dict = change_file_extensions_to_tif(each_file_dict, self.config.get("file-extensions-to-protect-from-changing-to-tif", []))
                    file_items.append(dict(each_file_dict))
        else:
            _fix_file_metadata_not_on_s3(file_items[index], parent_unique_identifier, self.config.get('media-file-extensions', []))
            file_items[index] = change_file_extensions_to_tif(file_items[index], self.config.get("file-extensions-to-protect-from-changing-to-tif", []))
        return file_items

    def _file_exists_in_list(self, file_items: list, id: str) -> bool:
        for item in file_items:
            if item.get("id", "") == id:
                return True
        return False

    def _remove_existing_file_from_list(self, file_items: list, id: str):
        for index, item in enumerate(file_items):
            if item.get("id", "") == id:
                file_items.pop(index)
                break


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
