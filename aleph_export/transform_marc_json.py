import json
from csv_from_json import CsvFromJson


class TransformMarcJson():
    """ This performs all Marc-related processing """
    def __init__(self, csv_field_names, hash_of_available_files):
        """ Save values required for all calls """
        self.json_control = read_marc_to_json_translation_control_file("./marc_to_json_translation_control_file.json")  # noqa: E501
        self.csv_from_json_class = CsvFromJson(csv_field_names, hash_of_available_files)

    def build_json_from_marc_json(self, marc_record_as_json):
        """ Build our own json object representing marc information we're interested in """
        json_record = {}
        for json_field_definition in self.json_control['root']['FieldsToExtract']:
            if 'label' in json_field_definition:
                json_record[json_field_definition['label']] = self._get_json_node_value_from_marc(json_field_definition, marc_record_as_json)  # noqa: E501
        return json_record

    def create_csv_from_json(self, json_record):
        """ Return csv string from json input """
        csv_string = self.csv_from_json_class.return_csv_from_json(json_record)
        return csv_string

    def _get_json_node_value_from_marc(self, json_field_definition, marc_record_as_json):
        """ Return an individual json node value from the json representation of the marc record. """
        results = ""
        if 'constant' in json_field_definition:
            results = json_field_definition.get("constant", "")
        else:
            results = self._get_value_from_marc_field(json_field_definition, marc_record_as_json)
        return results

    def _get_value_from_marc_field(self, json_field_definition, marc_record_as_json):
        """ More detailed logic to extract value from json representation of marc record,
            as defined by marc_to_json_translation_control_file.json """
        subfields_needed = json_field_definition.get("subfields", "")
        positions = json_field_definition.get("positions", "")
        format = json_field_definition.get("format", "")
        extra_processing = json_field_definition.get("extraProcessing", "")
        special_subfields = json_field_definition.get("specialSubfields", "")
        node = self._default_to_appropriate_data_type(format)
        for field in marc_record_as_json['fields']:
            for key, value in field.items():
                if self._process_this_field(json_field_definition, key, value):
                    if 'subfields' in value:
                        value_found = self._get_required_subfields(value, subfields_needed, special_subfields)
                    elif positions != "":
                        value_found = self._get_required_positions(value, positions)
                    else:
                        value_found = value
                    if value_found is not None and value_found != "":
                        if format == "text":
                            node = value_found  # redefine node as string here
                            break  # get out if we're only looking for a single value
                        else:
                            node.append(value_found)
            else:
                continue  # only executed if inner loop did NOT break
            break  # only executed if inner loop DID break
        if extra_processing != "":
            node = self._do_extra_processing(node, extra_processing)
        return node

    def _process_this_field(self, json_field_definition, key, value):
        """ This returns a boolean indicating whether or not this field should be processed """
        results = False
        fields = json_field_definition.get("fields", "")
        skip_fields = json_field_definition.get("skipFields", "")
        selection = json_field_definition.get("selection", "")
        if selection == 'range':
            start_of_range = fields[0]
            end_of_range = fields[len(fields) - 1]
            if key >= start_of_range and key <= end_of_range and key not in skip_fields:
                results = True
        elif key in fields and key not in skip_fields:
            results = True
        if results and 'verifySubfieldsMatch' in json_field_definition:
            # print("json_field_definition = ", results, json_field_definition)
            results = self._verify_subfields_match(json_field_definition['verifySubfieldsMatch'], value)
            # print("results from verify_subfields_match", results)
        return results

    def _verify_subfields_match(self, verify_subfields_match, subfields):
        """ For Marc field 956, we only want to harvest those records with a subfield for MARBLE.
            This returns a boolean value after applying this logic. """
        results = False
        # print("subfields = ", subfields)
        if 'subfields' in subfields:
            for subfield in subfields['subfields']:
                for key, value in subfield.items():
                    if key == verify_subfields_match['subfield'] and value == verify_subfields_match['value']:
                        results = True
        return results

    def _default_to_appropriate_data_type(self, format):
        """ Create a node with the appropriate data type defined in the control file. """
        node = []
        if format == "text":
            node = ""
        return node

    def _do_extra_processing(self, value, extra_processing):
        """ If extra processing is required, make appropriate calls to perform that additional processing. """
        results = ""
        if extra_processing == "link_to_source":
            results = "https://onesearch.library.nd.edu/primo-explore/fulldisplay?docid=ndu_aleph" + value + "&context=L&vid=NDU&lang=en_US&search_scope=malc_blended&adaptor=Local%20Search%20Engine&tab=onesearch&query=any,contains,ndu_aleph002097132&mode=basic"  # noqa: E501
        elif extra_processing == "lookup_work_type":
            results = self._lookup_work_type(value)
        elif extra_processing == "format_subjects":
            results = self._format_subjects(value)
        return results

    def _lookup_work_type(self, key_to_find):
        """ Worktype requires translation using this dictionary. """
        work_type_dict = {"a": "Language material",
                          "t": "Manuscript language material",
                          "m": "Computer file",
                          "e": "Cartographic material",
                          "f": "Manuscript cartographic material",
                          "p": "Mixed materials",
                          "i": "Nonmusical sound recording",
                          "j": "Musical sound recording",
                          "c": "Notated music",
                          "d": "Manuscript notated music",
                          "g": "Projected medium",
                          "k": "Two-dimensional nonprojected graphic",
                          "o": "Kit",
                          "r": "Three-dimensional artifact or naturally occuring object"
                          }
        return work_type_dict.get(key_to_find, "")

    def _format_subjects(self, value):
        """ Subjects require special formatting.  We may need to expand this to include URI if subfield 0 is added """
        results = []
        for each_value in value:
            node = {}
            if "^^^" in each_value:
                node["term"] = each_value.split("^^^")[0]
                node["uri"] = each_value.split("^^^")[1]
            else:
                node['term'] = each_value
            results.append(node)
        return results

    def _get_required_positions(self, value, positions_needed):
        """ Extract values from specific positions within a string """
        results = ""
        for position in positions_needed:
            if len(value) >= position:
                results += value[position]
        return results

    def _get_required_subfields(self, subfields, subfields_needed, special_subfields):
        """ Append values from subfields we're interested in """
        results = ""
        for subfield in subfields['subfields']:
            for key, value in subfield.items():
                if key in subfields_needed:
                    if results == "":
                        results = value
                    else:
                        results += " " + value
                if key in special_subfields:
                    results += "^^^" + value
        return results


def read_marc_to_json_translation_control_file(filename):
    """ Read json file which defines marc_json to json translation """
    try:
        with open(filename, 'r') as input_source:
            data = json.load(input_source)
        input_source.close()
    except IOError:
        print('Cannot open ' + filename)
        raise
    return data
