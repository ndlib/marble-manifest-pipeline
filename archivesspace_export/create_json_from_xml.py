# create_json_from_xml.py
import json
import os
from perform_additional_processing import perform_additional_processing
from additional_functions import return_None_if_needed, \
    get_seed_nodes_json, get_value_from_labels, remove_nodes_from_dictionary, \
    exclude_if_pattern_matches, strip_unwanted_whitespace
from pipelineutilities.validate_json import schema_api_version, validate_nd_json
from xml.etree import ElementTree


class createJsonFromXml():
    """ This class uses a control file (xml_to_json_translation_control_file.json)
        to guide translation from ArchivesSpace OAI xml to JSON.  We then validate
        the created json against our nd_json schema and either return validated json
        or an empty dictionary. """

    def __init__(self):
        local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"
        self.json_control = self._read_xml_to_json_translation_control_file(local_folder + "xml_to_json_translation_control_file.json")  # noqa: E501
        self.schema_api_version = schema_api_version()

    def get_nd_json_from_xml(self, xml_root: ElementTree) -> dict:
        """ Call function to recursively create json from xml root.  Validate, and return either validated json or {} """
        nd_json = {}
        if xml_root:
            nd_json = self.extract_fields(xml_root, 'root', {})
            if not validate_nd_json(nd_json):
                nd_json = {}
        return nd_json

    def extract_fields(self, xml_root: ElementTree, json_section: str, seeded_json_output: dict) -> dict:
        """ This code processes translations defined in the portion of the
            xml_to_json_translation_control_file.json named by the json_section passed.
            This calls get_node, which in turn calls extract_fields as needed. """
        json_output = seeded_json_output.copy()
        json_control_root = self.json_control[json_section]
        for field in json_control_root['FieldsToExtract']:
            value = self._process_extract_fields(field, json_output, xml_root)
            optional = field.get('optional', False)
            collapse_tree = field.get('collapseTree', False)
            if value is not None:
                if collapse_tree and len(value) > 0:
                    json_output.update(value[0])
                else:
                    if ((not optional) or len(value) > 0):
                        if 'label' in field:
                            json_output[field['label']] = value
        return json_output

    def _process_extract_fields(self, field: dict, json_output: dict, xml_root: ElementTree):
        """ This contains more of the detailed logic for the extract_fields function. """
        value = None
        if 'constant' in field:
            value = field.get('constant', '')
        elif 'fromLabels' in field:
            value = get_value_from_labels(json_output, field)
        elif 'externalProcess' in field:
            value = perform_additional_processing(json_output, field, self.schema_api_version)
        elif 'removeNodes' in field:
            value = remove_nodes_from_dictionary(json_output, field)
        else:
            seed_json = {}
            if 'seedNodes' in field:
                seed_nodes_control = field.get('seedNodes', '')
                seed_json = get_seed_nodes_json(json_output, seed_nodes_control)
            value = self._get_node(xml_root, field, seed_json)
            if field.get('format', '') == 'text' and value == []:
                value = ""
        return value

    def _get_node(self, xml: ElementTree, field: dict, seed_json: dict):
        """ This retrieves an individual value (or array) from XML
            , and saves to a JSON node or array.  If "otherNodes" are sepcified,
            call extract_fields to process those other nodes. """
        xpath = field.get('xpath', '')
        return_attribute_name = field.get('returnAttributeName', '')
        process_other_nodes = field.get('otherNodes', '')
        optional = field.get('optional', False)
        remove_duplicates = field.get('removeDuplicates', False)
        exclude_pattern = field.get('excludePattern', '')
        format = field.get('format', 'array')
        node = []
        for xml_item in xml.findall(xpath):
            value_found = ""
            if process_other_nodes > "":
                value_found = self.extract_fields(xml_item, process_other_nodes, seed_json)
                value_found = return_None_if_needed(value_found, field)
            else:
                value_found = get_xml_node_value(xml_item, return_attribute_name, exclude_pattern)
                if remove_duplicates and value_found:
                    if value_found in node:
                        value_found = None
            if not(optional and value_found is None):
                if format == "text":
                    node = value_found  # redefine node as string here if needed
                else:
                    node.append(value_found)
        return node

    def _read_xml_to_json_translation_control_file(self, filename: str) -> dict:
        """ Read json file which defines xml to json translation """
        try:
            with open(filename, 'r') as input_source:
                data = json.load(input_source)
        except IOError:
            print('Cannot open ' + filename)
            raise
        return data


def get_xml_node_value(item: ElementTree, return_attribute_name: str, exclude_pattern: list) -> str:
    """ This returns the xml text or attribute as specified
        and returns an empty string if not found."""
    if return_attribute_name in item.attrib:
        value_found = item.attrib[return_attribute_name]
    else:
        value_found = item.text
    value_found = exclude_if_pattern_matches(exclude_pattern, value_found)
    value_found = strip_unwanted_whitespace(value_found)
    return value_found
