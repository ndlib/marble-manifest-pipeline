# translate_curate_json_node.py
import os
import json
from datetime import datetime
# from get_bendo_info import get_bendo_info
from pipelineutilities.validate_json import schema_api_version, validate_standard_json
from do_extra_processing import do_extra_processing, get_seed_nodes_json


class TranslateCurateJsonNode():
    """ This translates a Curate Json node to our standard json based on curate_to_json_translation_control_file.json """
    def __init__(self, config):
        self.config = config
        local_folder = os.path.dirname(os.path.realpath(__file__))
        self.json_control = read_curate_to_json_translation_control_file(local_folder + "/curate_to_json_translation_control_file.json")  # noqa: E501
        self.schema_api_version = schema_api_version()

    def build_json_from_curate_json(self, curate_json: dict, translation_branch: str, seeded_json: dict) -> dict:
        """ Build our own json object representing Curate information we're interested in """
        standard_json = seeded_json.copy()
        if translation_branch in self.json_control:
            for json_field_definition in self.json_control[translation_branch]['FieldsToExtract']:
                if 'label' in json_field_definition:
                    results = self._get_json_node_value_from_curate(json_field_definition, curate_json, standard_json)  # noqa: E501
                    if results:
                        if isinstance(results, list):
                            if json_field_definition['label'] not in standard_json:
                                standard_json[json_field_definition['label']] = results
                            else:
                                # Note:  We may already have items (for children), and need to append more items (for files)
                                standard_json[json_field_definition['label']].append(results)
                        else:
                            standard_json[json_field_definition['label']] = results
        if not validate_standard_json(standard_json):
            standard_json = {}
        return standard_json

    def _get_json_node_value_from_curate(self, json_field_definition: dict, curate_json: dict, standard_json: dict) -> dict or str or list:
        """ Return an individual json node value from the json representation of the curate record. """
        results = ""
        seed_json = {}
        if 'seedNodes' in json_field_definition:
            seed_nodes_control = json_field_definition.get('seedNodes', '')
            seed_json = get_seed_nodes_json(standard_json, seed_nodes_control)
        if 'constant' in json_field_definition:
            results = json_field_definition.get("constant", "")
        elif 'otherNodes' in json_field_definition:
            results = self._process_other_nodes(json_field_definition, curate_json, standard_json)
        elif 'externalProcess' in json_field_definition:
            parameters_json = get_seed_nodes_json(curate_json, json_field_definition.get('passSourceNodes', ''))
            results = self._execute_external_process(json_field_definition, parameters_json, standard_json, seed_json)
        else:
            results = self._get_value_from_curate_field(json_field_definition, curate_json, standard_json)
        return results

    def _process_other_nodes(self, json_field_definition: dict, curate_json: dict, standard_json: dict) -> dict:
        results = None
        fields = json_field_definition.get("fields", "")
        all_required_fields_exist = True
        for field in fields:
            if field not in curate_json:
                all_required_fields_exist = False
                break
        if all_required_fields_exist:
            seed_json = {}
            if 'seedNodes' in json_field_definition:
                seed_nodes_control = json_field_definition.get('seedNodes', '')
                seed_json = get_seed_nodes_json(standard_json, seed_nodes_control)
            preliminary_results = self.build_json_from_curate_json(curate_json, json_field_definition.get('otherNodes', ''), seed_json)
            if json_field_definition.get("format", "") == "array":
                results = []
                results.append(preliminary_results)
            else:
                results = preliminary_results
        return results

    def _get_value_from_curate_field(self, json_field_definition: dict, curate_json: dict, standard_json: dict):
        """ More detailed logic to extract value from json representation of curate record,
            as defined by curate_to_json_translation_control_file.json """
        format = json_field_definition.get("format", "")
        fields = json_field_definition.get("fields", "")
        date_pattern = json_field_definition.get("datePattern", "")
        extra_processing = json_field_definition.get("extraProcessing", "")
        node = self._default_to_appropriate_data_type(format)
        for field in fields:
            value = curate_json.get(field, None)
            if not value:
                value = None
            if value is not None and value != "":
                if format == "text":
                    if isinstance(value, list):  # this is to fix those cases where Curate sometimes has an array of strings, instead of a string
                        for each_value in value:
                            value = each_value
                            break
                    if date_pattern:
                        value = value.replace("Z", "")
                        value = datetime.strptime(value, date_pattern).isoformat() + 'Z'
                    node = value  # redefine node as string here
                    break  # get out if we're only looking for a single value
                else:
                    node.append(value)
        if extra_processing != "":
            node = do_extra_processing(node, extra_processing, json_field_definition, self.config.get("bendo-server-base-url", ""), self.schema_api_version, standard_json)
        return node

    def _default_to_appropriate_data_type(self, format: str) -> str or list:
        """ Create a node with the appropriate data type defined in the control file. """
        node = []
        if format == "text":
            node = ""
        return node

    def _execute_external_process(self, json_field_definition: dict, parameters_json: dict, standard_json: dict, seed_json: dict) -> list:
        external_process = json_field_definition.get("externalProcess", "")
        results = self._default_to_appropriate_data_type(json_field_definition.get("format", ""))
        if external_process == 'process_contained_files':
            if 'containedFiles' in parameters_json:
                contained_files_json = parameters_json["containedFiles"]
                files_json_array = self._process_contained_files(contained_files_json, seed_json)
                results.extend(files_json_array)
        return results

    def _process_contained_files(self, contained_files_json: dict, seed_json: dict) -> list:
        """ For each file listed, retrieve information, including md5Checksum from Bendo """
        results = []
        sequence = 1
        for file_json in contained_files_json:
            json_results = self.build_json_from_curate_json(file_json, "containedFiles", seed_json)
            # print("json_results = ", json_results)
            json_results["sequence"] = sequence
            sequence += 1
            # May need to re-instate this code - keep for now.
            # if json_results.get("md5Checksum", "") == "" and \
            #         json_results.get("bendoItem", "") > "" and \
            #         json_results.get("title", "") > "":
            #     bendo_info = get_bendo_info(self.config.get("bendo-server-base-url", ""), json_results["bendoItem"], json_results["title"])
            #     json_results["md5Checksum"] = bendo_info.get("X-Content-Md5", "")
            results.append(json_results)
        return results


def read_curate_to_json_translation_control_file(filename: str) -> dict:
    """ Read json file which defines curate_json to json translation """
    try:
        with open(filename, 'r') as input_source:
            data = json.load(input_source)
        input_source.close()
    except IOError:
        print('Cannot open ' + filename)
        raise
    return data
