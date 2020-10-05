import json
import os
from datetime import datetime
from search_files import id_from_url, crawl_available_files  # noqa: #402


class AddFilesToJsonObject():
    def __init__(self, config: dict):
        self.config = config
        parent_folder = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + "/"
        if config.get("local", False):
            with open(parent_folder + 'test/hash_of_available_files.json', 'r') as input_source:
                self.hash_of_available_files = json.load(input_source)
        else:
            self.hash_of_available_files = crawl_available_files(self.config)
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
                        self._add_other_files_given_uri(standard_json["items"], index, file_path)
                        break  # assumption is that we process only the first item to find additional files
        return standard_json

    def _add_other_files_given_uri(self, file_items: list, index: int, file_path: str):
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
                    each_file_dict['objectFileGroupId'] = id_to_find
                    each_file_dict['collectionId'] = collection_id
                    each_file_dict['sourceSystem'] = source_system
                    each_file_dict['repository'] = repository
                    each_file_dict['apiVersion'] = api_version
                    each_file_dict['fileCreatedDate'] = file_created_date
                    each_file_dict['level'] = 'file'
                    each_file_dict['parentId'] = parent_id
                    each_file_dict['id'] = os.path.basename(obj['key'])
                    each_file_dict['thumbnail'] = (each_file_dict['id'] == item_id)
                    each_file_dict['description'] = ""
                    if each_file_dict['id'] == item_id:
                        each_file_dict['description'] = item_description
                    each_file_dict['filePath'] = obj['path']
                    sequence += 1
                    each_file_dict['sequence'] = obj.get('order', sequence)
                    each_file_dict['title'] = obj['label']
                    if obj['lastModified']:
                        each_file_dict['modifiedDate'] = obj['lastModified']
                        if isinstance(each_file_dict['modifiedDate'], str):
                            obj['lastModified'] = obj['lastModified'].replace("+00:00", "")
                            each_file_dict['modifiedDate'] = datetime.strptime(obj['lastModified'], '%Y-%m-%d %H:%M:%S').isoformat() + '+00:00Z'
                        elif isinstance(each_file_dict['modifiedDate'], datetime):
                            each_file_dict['modifiedDate'] = obj['lastModified'].isoformat() + 'Z'
                    each_file_dict['md5Checksum'] = obj['eTag'].replace("'", "").replace('"', '')  # strip duplicated quotes: {'ETag': '"8b50cfed39b7d8bcb4bd652446fe8adf"'}  # noqa: E501
                    if self._file_exists_in_list(file_items, each_file_dict['id']):
                        self._remove_existing_file_from_list(file_items, each_file_dict['id'])
                    file_items.append(dict(each_file_dict))
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
