import json
from datetime import datetime
from pipelineutilities.search_files import id_from_url, crawl_available_files  # noqa: #402
from additional_functions import file_name_from_filePath


class AddFilesToNdJson():
    def __init__(self, config: dict):
        self.config = config
        self.processing_dao_for_parent_id = ""
        self.hash_of_available_files = crawl_available_files(self.config)
        # with open('hash_of_available_files.json', 'w') as f:
        #     json.dump(self.hash_of_available_files, f, indent=2, default=str)

    def add_files(self, nd_json: dict) -> dict:
        """ recursively go through all of nd_json finding files, and adding additional files """
        if "items" in nd_json:
            for index, item in enumerate(nd_json["items"]):
                if item.get("level", "") in ["manifest", "collection"]:
                    self.add_files(item)
                elif item.get("level", "") == "file":
                    file_path = item.get("filePath", "")
                    if file_path:
                        self._add_other_files_given_uri(nd_json["items"], index, file_path)
                        break  # assumption is that we process only the first item to find additional files
        return nd_json

    def _add_other_files_given_uri(self, file_items: list, index: int, file_path: str):
        """ This accepts an image uri, and finds (and appends to the csv)
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
                for obj in self.hash_of_available_files[id_to_find]['files']:
                    each_file_dict['collectionId'] = collection_id
                    each_file_dict['sourceSystem'] = source_system
                    each_file_dict['repository'] = repository
                    each_file_dict['apiVersion'] = api_version
                    each_file_dict['fileCreatedDate'] = file_created_date
                    each_file_dict['parentId'] = parent_id
                    each_file_dict['id'] = file_name_from_filePath(obj['Key'])
                    each_file_dict['thumbnail'] = (each_file_dict['id'] == item_id)
                    each_file_dict['description'] = ""
                    if each_file_dict['id'] == item_id:
                        each_file_dict['description'] = item_description
                    each_file_dict['filePath'] = obj['Path']
                    each_file_dict['sequence'] = obj['Order']
                    each_file_dict['title'] = obj['Label']
                    each_file_dict['modifiedDate'] = obj['LastModified']
                    each_file_dict['modifiedDate'] = datetime.strptime(obj['LastModified'], '%Y-%m-%d %H:%M:%S').isoformat() + 'Z'  # noqa: E501
                    each_file_dict['md5Checksum'] = obj['ETag'].replace("'", "").replace('"', '')  # strip duplicated quotes: {'ETag': '"8b50cfed39b7d8bcb4bd652446fe8adf"'}  # noqa: E501
                    # print("appending dict")
                    file_items.append(dict(each_file_dict))
        return file_items
    #
    # def _is_this_first_dao_in_object(self, my_parent_id):
    #     """ This identifies the first DAO image in a DAO object
    #         so we can use this as the representative thumbnail. """
    #     return_value = True
    #     if my_parent_id == self.processing_dao_for_parent_id:
    #         return_value = False
    #     self.processing_dao_for_parent_id = my_parent_id
    #     return return_value
