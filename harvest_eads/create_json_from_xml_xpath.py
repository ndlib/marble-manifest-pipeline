# create_json_from_xml_xpath.py
import json
import os
import sys
from datetime import datetime
from file_system_utilities import create_directory
from additional_functions import check_for_inconsistent_dao_image_paths, file_name_from_filePath, \
    define_manifest_level, get_repository_name_from_ead_resource, return_None_if_needed, \
    get_seed_nodes_json, get_xml_node_value, get_value_from_labels, remove_nodes_from_dictionary, \
    enforce_required_descendants, exclude_if_pattern_matches, strip_unwanted_whitespace, \
    get_json_value_as_string, format_creators
from output_csv import OutputCsv
where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am)
sys.path.append(where_i_am + "/dependencies")
from dependencies.pipelineutilities.search_files import id_from_url, crawl_available_files  # noqa: #402
from dependencies.pipelineutilities.s3_helpers import write_s3_file  # noqa: E402


class createJsonFromXml():
    """ This class uses a control file (xml_to_json_translation_control_file.json)
        to guide translation from ArchivesSpace OAI xml to JSON.  We then save
        the contents of that JSON into CSV files.  The fields for these CSV files
        are defined in config['csv-field-names'] """

    def __init__(self, config, json_control):
        self.config = config
        self.json_control = json_control
        self.save_to_s3 = True
        self.delete_local_copy = False
        self.hash_of_available_files = crawl_available_files(self.config)
        self.processing_dao_for_parent_id = ""
        self.temporary_local_path = "/tmp"
        self.sequences_within_parent = {}
        self.output_csv_class = OutputCsv(self.config["csv-field-names"])

    def extract_fields(self, xml_root, json_section, seeded_json_output):
        """ This code processes translations defined in the portion of the
            xml_to_json_translation_control_file.json named by the json_section passed.
            This calls get_node, which in turn calls extract_fields as needed. """
        if json_section == 'root':
            self.output_csv_class.write_csv_header()
        json_output = seeded_json_output.copy()
        json_control_root = self.json_control[json_section]
        section_required_descendants = get_json_value_as_string(json_control_root, 'requiredDescendants')
        for field in json_control_root['FieldsToExtract']:
            value = self._process_extract_fields(field, json_output, xml_root)
            if 'saveRecordAsSeparateFile' in field or 'saveCsvRecord' in field:
                return value  # early out
            optional = get_json_value_as_string(field, 'optional')
            collapse_tree = get_json_value_as_string(field, 'collapseTree')
            required_descendants = get_json_value_as_string(field, 'requiredDescendants')
            value = enforce_required_descendants(value, required_descendants)
            if value is not None:
                if collapse_tree and len(value) > 0:
                    json_output.update(value[0])
                else:
                    if ((not optional) or len(value) > 0):
                        if 'label' in field:
                            json_output[field['label']] = value
        json_output = enforce_required_descendants(json_output, section_required_descendants)
        return json_output

    def _process_extract_fields(self, field, json_output, xml_root):
        """ This contains more of the detailed logic for the extract_fields function. """
        value = None
        if 'constant' in field:
            value = get_json_value_as_string(field, 'constant')
        elif 'fromLabels' in field:
            value = get_value_from_labels(json_output, field)
        elif 'externalProcess' in field:
            value = self._call_external_process(json_output, field)
        elif 'saveRecordAsSeparateFile' in field:
            value = self._save_record_as_separate_file(json_output, field)
            return value  # early out
        elif 'saveCsvRecord' in field:
            value = self._save_csv_record(json_output, field)
            return value  # early out
        elif 'removeNodes' in field:
            value = remove_nodes_from_dictionary(json_output, field)
        else:
            seed_json = {}
            if 'seedNodes' in field:
                seed_nodes_control = get_json_value_as_string(field, 'seedNodes')
                seed_json = get_seed_nodes_json(json_output, seed_nodes_control)
            value, inconsistency_found_flag = self._get_node(xml_root, field, seed_json)
            if inconsistency_found_flag:
                print("after call to _get_node for field ", field)
            if get_json_value_as_string(field, 'format') == 'text' and value == []:
                value = ""
        return value

    def _save_csv_record(self, json_node, field):
        """ Append json_node information to the csv_writer and then save to s3. """
        return_node = {}
        required_descendants = get_json_value_as_string(field, 'requiredDescendants')
        json_node = enforce_required_descendants(json_node, required_descendants)
        if json_node is not None:
            json_node['sequence'] = self._accumulate_sequences_by_parent(json_node)
            self.output_csv_class.write_csv_row(json_node)
            if 'fileNamedForNode' in field:
                if field['fileNamedForNode'] in json_node:
                    csv_file_name = json_node[field['fileNamedForNode']] + '.csv'
                    s3_file_name = os.path.join(self.config['process-bucket-csv-basepath'], csv_file_name)
                    write_s3_file(self.config['process-bucket'], s3_file_name, self.output_csv_class.return_csv_value())  # noqa: #501
            retain_only_nodes = get_json_value_as_string(field, 'retainOnlyNodes')
            if retain_only_nodes == "":
                return_node = json_node
            else:
                for return_node_name in retain_only_nodes:
                    if return_node_name in json_node:
                        return_node[return_node_name] = json_node[return_node_name]
        return return_node

    def _accumulate_sequences_by_parent(self, json_node):
        sequence_to_use = 1
        my_parent_id = json_node['parentId']
        if my_parent_id != 'root' and my_parent_id in self.sequences_within_parent:
            sequence_to_use = self.sequences_within_parent[my_parent_id] + 1
        self.sequences_within_parent[my_parent_id] = sequence_to_use
        return sequence_to_use

    def _save_record_as_separate_file(self, json_node, field):
        """ This gives us the opportunity to save the json file.
            This can be helpful in debugging. """
        return_node = {}
        file_name = ""
        required_descendants = get_json_value_as_string(field, 'requiredDescendants')
        json_node = enforce_required_descendants(json_node, required_descendants)
        if json_node != {} and json_node is not None:
            file_named_for_node = get_json_value_as_string(field, 'fileNamedForNode')
            if file_named_for_node in json_node:
                file_name = json_node[file_named_for_node] + '.json'
            if file_name != "" and json_node is not None:
                self.save_json_record(file_name, json_node)
            retain_only_nodes = get_json_value_as_string(field, 'retainOnlyNodes')
            if retain_only_nodes == "":
                return_node = json_node
            else:
                for return_node_name in retain_only_nodes:
                    if return_node_name in json_node:
                        return_node[return_node_name] = json_node[return_node_name]
        return return_node

    def _get_node(self, xml, field, seed_json):
        """ This retrieves an individual value (or array) from XML
            , and saves to a JSON node or array.  If "otherNodes" are sepcified,
            call extract_fields to process those other nodes. """
        inconsistency_found_flag = False
        xpath = get_json_value_as_string(field, 'xpath')
        return_attribute_name = get_json_value_as_string(field, 'returnAttributeName')
        process_other_nodes = get_json_value_as_string(field, 'otherNodes')
        optional = get_json_value_as_string(field, 'optional')
        remove_duplicates = get_json_value_as_string(field, 'removeDuplicates')
        exclude_pattern = get_json_value_as_string(field, 'excludePattern')
        format = get_json_value_as_string(field, 'format')
        node = []
        for item in xml.findall(xpath):
            value_found = ""
            if process_other_nodes > "":
                value_found = self.extract_fields(item, process_other_nodes, seed_json)
                required_descendants = get_json_value_as_string(field, 'requiredDescendants')
                value_found = enforce_required_descendants(value_found, required_descendants)
                value_found = return_None_if_needed(value_found, field)
            else:
                value_found = get_xml_node_value(item, return_attribute_name)
                value_found = exclude_if_pattern_matches(exclude_pattern, value_found)
                value_found = strip_unwanted_whitespace(value_found)
                if remove_duplicates and value_found is not None:
                    if value_found in node:
                        value_found = None
            if not(optional and value_found is None):
                if format == "text":
                    node = value_found  # redefine node as string here
                else:
                    node.append(value_found)
            if process_other_nodes > "":
                inconsistency_found_flag = check_for_inconsistent_dao_image_paths(field, node)
                if inconsistency_found_flag:
                    print("after check_for_inconsistent_dao_image_paths")
        return node, inconsistency_found_flag

    def save_json_record(self, file_name, json_to_save):
        """ This lets us save the json record locally, for debugging purposes. """
        local_folder = self.temporary_local_path
        create_directory(local_folder)
        local_file_name = os.path.join(local_folder, file_name)
        with open(local_file_name, 'w') as f:
            json.dump(json_to_save, f, indent=2)
        return

    def _call_external_process(self, json_node, field):  # noqa: C901
        """ This lets us call other named functions to do additional processing. """
        return_value = ""
        external_process_name = get_json_value_as_string(field, 'externalProcess')
        parameters_json = {}
        if 'passLabels' in field:
            parameters_json = get_seed_nodes_json(json_node, field['passLabels'])
        if external_process_name == 'id_from_url':
            if 'filename' in parameters_json:
                return_value = id_from_url(parameters_json['filename'])
        elif external_process_name == 'getFilesFromUri':
            return_value = self._process_get_files_from_uri(json_node, field, parameters_json)
        elif external_process_name == 'get_repository_name_from_ead_resource':
            if 'resource' in parameters_json:
                return_value = get_repository_name_from_ead_resource(parameters_json['resource'])
        elif external_process_name == 'define_level':
            return_value = 'manifest'
            if 'children' in parameters_json:
                return_value = define_manifest_level(parameters_json['children'])
        elif external_process_name == 'file_name_from_filePath':
            if 'filename' in parameters_json:
                return_value = file_name_from_filePath(parameters_json['filename'])
        elif external_process_name == "format_creators":
            if 'creator' in parameters_json:
                return_value = format_creators(parameters_json["creator"])
        return return_value

    def _process_get_files_from_uri(self, json_node, field, parameters_json):
        """ This accepts an image uri, and finds (and appends to the csv)
            all related images. """
        if 'filename' in parameters_json:
            if not self._is_this_first_dao_in_object(json_node['parentId']):
                return
            each_file_dict = {}
            if 'seedNodes' in field:
                seed_nodes_control = get_json_value_as_string(field, 'seedNodes')
                each_file_dict = get_seed_nodes_json(json_node, seed_nodes_control)
            uri = parameters_json['filename']
            id = id_from_url(uri)
            if id in self.hash_of_available_files:
                if 'files' in self.hash_of_available_files[id]:
                    for obj in self.hash_of_available_files[id]['files']:
                        each_file_dict['id'] = file_name_from_filePath(obj['Key'])
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
                        self.output_csv_class.write_csv_row(each_file_dict)
        return None

    def _is_this_first_dao_in_object(self, my_parent_id):
        """ This identifies the first DAO image in a DAO object
            so we can use this as the representative thumbnail. """
        return_value = True
        if my_parent_id == self.processing_dao_for_parent_id:
            return_value = False
        self.processing_dao_for_parent_id = my_parent_id
        return return_value
