from datetime import datetime
from output_csv import OutputCsv
from dependencies.pipelineutilities.search_files import id_from_url


class CsvFromJson():
    """ This performs all Marc-related processing """
    def __init__(self, csv_field_names, hash_of_available_files):
        """ Save values required for all calls """
        self.csv_field_names = csv_field_names
        self.hash_of_available_files = hash_of_available_files

    def return_csv_from_json(self, json_record):
        """ Given marc_record_as_json, create the csv describing that object, including related file information. """
        if json_record:
            output_csv_class = OutputCsv(self.csv_field_names)
            output_csv_class.write_csv_row(json_record)
            image_uri = json_record.get("imageUri", "")
            if image_uri != "":
                self._process_get_files_from_uri(json_record,
                                                 image_uri,
                                                 output_csv_class)
            csv_string = output_csv_class.return_csv_value()
        return csv_string

    def _process_get_files_from_uri(self, json_node, file_uri, output_csv_class):
        """ This accepts an image uri, and finds (and appends to the csv)
            all related images. """
        each_file_dict = {}
        id = id_from_url(file_uri)
        print("id_from_url = ", id, " file_uri = ", file_uri)
        each_file_dict['collectionId'] = json_node['collectionId']
        each_file_dict['level'] = 'file'
        each_file_dict['parentId'] = json_node['id']
        each_file_dict['fileId'] = id
        each_file_dict['sourceSystem'] = json_node['sourceSystem']
        each_file_dict['repository'] = json_node['repository']
        if id in self.hash_of_available_files:
            if 'files' in self.hash_of_available_files[id]:
                for obj in self.hash_of_available_files[id]['files']:
                    each_file_dict['id'] = self._file_name_from_filePath(obj['Key'])
                    each_file_dict['thumbnail'] = (each_file_dict['id'] == json_node['id'])
                    each_file_dict['description'] = None
                    if each_file_dict['id'] == json_node['id']:
                        each_file_dict['description'] = json_node['description']
                    each_file_dict['fileInfo'] = obj
                    each_file_dict['filePath'] = obj['Path']
                    each_file_dict['sequence'] = obj['Order']
                    each_file_dict['title'] = obj['Label']
                    each_file_dict['modifiedDate'] = obj['LastModified']
                    each_file_dict['modifiedDate'] = datetime.strptime(obj['LastModified'], '%Y-%m-%d %H:%M:%S').isoformat() + 'Z'  # noqa: E501
                    each_file_dict['md5Checksum'] = obj['ETag'].replace("'", "").replace('"', '')  # strip duplicated quotes: {'ETag': '"8b50cfed39b7d8bcb4bd652446fe8adf"'}  # noqa: E501
                    output_csv_class.write_csv_row(each_file_dict)
        return None

    def _file_name_from_filePath(self, file_path):
        """ Retrieve just the file name given the complete file path."""
        split_file_path = file_path.split('/')
        return_value = split_file_path[len(split_file_path) - 1]
        return return_value
