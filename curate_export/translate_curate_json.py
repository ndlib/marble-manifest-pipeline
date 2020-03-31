# curate_api.py
import os
import json
import time
from xml.etree import ElementTree
from datetime import datetime
# import dependencies.requests
# from dependencies.sentry_sdk import capture_exception


class TranslateCurateJson():
    """ This performs all Curate API processing """
    def __init__(self, config, event):
        self.config = config
        self.event = event
        self.start_time = time.time()
        local_folder = os.path.dirname(os.path.realpath(__file__))
        self.json_control = read_curate_to_json_translation_control_file(local_folder + "/curate_to_json_translation_control_file.json")  # noqa: E501

    def build_json_from_curate_json(self, curate_json, translation_branch="root"):
        """ Build our own json object representing Curate information we're interested in """
        json_record = {}
        for json_field_definition in self.json_control[translation_branch]['FieldsToExtract']:
            if 'label' in json_field_definition:
                results = self._get_json_node_value_from_curate(json_field_definition, curate_json)  # noqa: E501
                json_record[json_field_definition['label']] = results
        return json_record

    def _get_json_node_value_from_curate(self, json_field_definition, curate_json):
        """ Return an individual json node value from the json representation of the curate record. """
        results = ""
        if 'constant' in json_field_definition:
            results = json_field_definition.get("constant", "")
        else:
            results = self._get_value_from_curate_field(json_field_definition, curate_json)
        return results

    def _get_value_from_curate_field(self, json_field_definition, curate_json):
        """ More detailed logic to extract value from json representation of curate record,
            as defined by curate_to_json_translation_control_file.json """
        format = json_field_definition.get("format", "")
        fields = json_field_definition.get("fields", "")
        date_pattern = json_field_definition.get("datePattern", "")
        extra_processing = json_field_definition.get("extraProcessing", "")
        node = self._default_to_appropriate_data_type(format)
        for field in fields:
            value = curate_json.get(field, None)
            if value is not None and value != "":
                if format == "text":
                    if date_pattern:
                        value = value.replace("Z", "")
                        value = datetime.strptime(value, date_pattern).isoformat() + 'Z'
                    node = value  # redefine node as string here
                    break  # get out if we're only looking for a single value
                else:
                    node.append(value)
        if extra_processing != "":
            node = self._do_extra_processing(node, extra_processing, json_field_definition)
        return node

    def _default_to_appropriate_data_type(self, format):
        """ Create a node with the appropriate data type defined in the control file. """
        node = []
        if format == "text":
            node = ""
        return node

    def _do_extra_processing(self, value, extra_processing, json_field_definition):
        """ If extra processing is required, make appropriate calls to perform that additional processing. """
        results = ""
        if extra_processing == "link_to_source":
            results = value.replace("/api/items/", "/show/")
        elif extra_processing == "format_creators":
            results = self._format_creators(value)
        elif extra_processing == "process_contained_files":
            results = self._process_contained_files(value)
        elif extra_processing == "extract_field_from_characterization_xml":
            fieldToExtract = json_field_definition.get("fieldToExtract", None)
            results = self._extract_field_from_characterization_xml(value, fieldToExtract)
        return results

    def _format_creators(self, value):
        """ Creators require special formatting.
            [{"attribution": "", "role": "Primary", "fullName": "Harvey, M.A."}]
            currently looks like this: [Abner Shimony](https://lccn.loc.gov/n78019978)
            """
        results = []
        for each_value in value:
            node = {}
            node["attribution"] = ""
            node["role"] = "Primary"
            each_value = each_value.replace("[", "").replace("]", "")
            if "(http" in each_value:
                node["fullName"] = each_value.split("(http")[0]
                node["uri"] = "http" + each_value.split("(http")[1].replace(")", "")
            else:
                node['fullName'] = each_value
            results.append(node)
        return results

    def _process_contained_files(self, contained_files_json):
        results = []
        sequence = 1
        for file_json_list in contained_files_json:  # for some reason, this is a list within a list
            for file_json in file_json_list:
                json_results = self.build_json_from_curate_json(file_json, "containedFiles")
                json_results["sequence"] = sequence
                sequence += 1
                results.append(json_results)
        return results

    def _extract_field_from_characterization_xml(self, characterization_string, field_to_extract):
        characterization_string = self._strip_namespaces(characterization_string)
        results = ""
        if isinstance(characterization_string, list):
            characterization_string = characterization_string[0]  # convert to a string
        if characterization_string:
            try:
                xml = ElementTree.fromstring(characterization_string)
                xml = ElementTree.ElementTree(xml)  # make the type an ElementTree instead of an Element
                results = xml.find(field_to_extract).text
            except xml.etree.ElementTree.ParseError:
                pass  # xml was not well formed.
        return results

    def _strip_namespaces(self, xml_string):
        """ In order to simplify xml harvest, we must strip these namespaces """
        namespaces_to_strip = ["ns2:", "ns1:", "ns0:",
                               "xmlns=\"http://hul.harvard.edu/ois/xml/ns/fits/fits_output\"",
                               "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://hul.harvard.edu/ois/xml/ns/fits/fits_output http://hul.harvard.edu/ois/xml/xsd/fits/fits_output.xsd\" version=\"0.6.2\" timestamp=\"12/11/18 8:50 AM\"",  # noqa: E501
                               "\n"
                              ]
        for string in namespaces_to_strip:
            xml_string = xml_string.replace(string, "")
        return xml_string


def read_curate_to_json_translation_control_file(filename):
    """ Read json file which defines curate_json to json translation """
    try:
        with open(filename, 'r') as input_source:
            data = json.load(input_source)
        input_source.close()
    except IOError:
        print('Cannot open ' + filename)
        raise
    return data
