# do_extra_processing.py

from datetime import date
from extract_field_from_characterization_xml import extract_field_from_characterization_xml


def do_extra_processing(value: str or dict, extra_processing: str, json_field_definition: dict, bendo_server_base_url: str, schema_api_version: int, standard_json: dict) -> str or dict or list or int:  # noqa: C901
    """ If extra processing is required, make appropriate calls to perform that additional processing. """
    results = ""
    parameters_json = {}
    parameters_json = get_seed_nodes_json(standard_json, json_field_definition.get('passLabels', ''))
    if extra_processing == "link_to_source":
        results = value.replace("/api/items/", "/show/")
        # results = value.replace("/api/items/", "/downloads/")
    elif extra_processing == "format_creators":
        results = _format_creators(value)
    elif extra_processing == "format_subjects":
        results = _format_subjects(value)
    elif extra_processing == "format_publisher":
        results = _format_publisher(value)
    elif extra_processing == "extract_field_from_characterization_xml":
        fieldToExtract = json_field_definition.get("fieldToExtract", None)
        results = extract_field_from_characterization_xml(value, fieldToExtract)
    elif extra_processing == 'schema_api_version':
        results = schema_api_version
    elif extra_processing == 'file_created_date':
        results = str(date.today())
    elif extra_processing == 'pick_created_date':
        results = _pick_created_date(parameters_json)
    elif extra_processing == 'pick_geographic_location':
        results = _pick_geographic_location(parameters_json)
    elif extra_processing == 'pick_access':
        results = _pick_access(parameters_json)
    elif extra_processing == 'pick_repository':
        results = _pick_repository(parameters_json)
    elif extra_processing == 'define_level':
        results = 'manifest'
        if 'items' in parameters_json:
            results = define_manifest_level(parameters_json['items'])
    elif extra_processing == 'translate_work_type':
        results = _translate_work_type(parameters_json)
    elif extra_processing == 'update_publisher':
        results = _update_publisher(parameters_json)
    return results


def _format_subjects(value: str) -> dict:
    """ Subjects require special formatting.  """
    results = []
    if value:
        node = {}
        node["term"] = value
        results.append(node)
    return results


def _format_publisher(value: str) -> dict:
    """ Publisher requires special formatting.  """
    results = {}
    # for those cases where Curate has a list, choose the first string in that list.
    if isinstance(value, list):
        for each_value in value:
            value = each_value
            break
    if value:
        if value == 'University of Notre Dame::School of Architecture':
            value = 'Architecture Library, Hesburgh Libraries'
        results["publisherName"] = value
    return results


def _update_publisher(parameters_json: dict) -> str:
    """ return 'Architecture Library, Hesburgh Libraries' for Architectural Lantern Slides,
        'University Archives, Hesburgh Libraries for Commencement Programs,
        else value from publisher """
    results = {}
    part_of = parameters_json.get('partOf', '')
    while isinstance(part_of, list):
        part_of = part_of[0]
    if "qz20sq9094h" in part_of:  # Architectural Lantern Slides return Architecture Library
        value = 'Architecture Library, Hesburgh Libraries'
        results["publisherName"] = value
        return results
    if "zp38w953h0s" in part_of or parameters_json.get('id', '') == 'zp38w953h0s': # Commencement Programs return University Archives  
        value = 'University Archives, Hesburgh Libraries'
        results["publisherName"] = value
        return results
    return parameters_json.get('publisher', '')


def _format_creators(value: list) -> list:
    """ Creators require special formatting.
        [{"attribution": "", "role": "Primary", "fullName": "Harvey, M.A."}]
        currently looks like this: [Abner Shimony](https://lccn.loc.gov/n78019978)
        Note:  in Curate, if there is an individual contributor, that is stored as a string.
            If there are multiple contributors, that is stored as an array.
            Because we define the creators format as array, we translate strings into array,
                and array into an array of arrays.
            We need to deal with both here.
        """
    results = []
    if isinstance(value, str):
        node = _format_creators_given_string(value)
        results.append(node)
    elif isinstance(value, list):
        for each_value in value:
            node = _format_creators(each_value)
            results.extend(node)
    return results


def _format_creators_given_string(contributors_string: str) -> dict:
    node = {}
    # node["attribution"] = ""
    # node["role"] = "Primary"
    if "(http" in contributors_string:
        node["fullName"] = contributors_string.split("(http")[0]
        node["uri"] = "http" + contributors_string.split("(http")[1].replace(")", "")
    else:
        if contributors_string == "":
            contributors_string = "unknown"
        node['fullName'] = contributors_string
    node["display"] = node.get("fullName", "")
    return node


def _pick_created_date(parameters_json: dict) -> str:
    """ Modified 9/28/21 to include additional logic based on collection.  Otherwise return first occurrance of: date, or created, or dateSubmitted """
    if 'Notre Dame Commencement Program: ' in parameters_json.get('title', ''):
        return parameters_json['title'].replace('Notre Dame Commencement Program: ', '')
    part_of = parameters_json.get('partOf', '')
    while isinstance(part_of, list):
        part_of = part_of[0]
    if 'qz20sq9094h' in part_of:  # Architectural Lantern Slides need a date of Circe 1910
        if parameters_json.get('id', '') != 'qz20sq9094h':  # But we won't overwrite the date in the root record of that collection
            return 'Circa 1910'
    return parameters_json.get('date', parameters_json.get('created', parameters_json.get('dateSubmitted', None)))


def _pick_geographic_location(parameters_json: dict) -> list:
    """ Sometimes, curate has an array, and sometimes an array within an array, where the inner-most array will have strings.  We need to accommodate both. """
    """ placeOfCreation has this format: ["Ani, Kars Province, Turkey",  " +40.507500+43.572777.", "Ani"].  We will choose the first one that contains a comma"""
    if 'placeOfCreation' not in parameters_json:
        return []
    for place_string in parameters_json.get("placeOfCreation", []):
        if isinstance(place_string, list):
            if len(place_string) == 0:
                return []
            place_string = place_string[0]
        if ',' in place_string:
            return [{"display": place_string}]
    return []


def _pick_access(parameters_json: dict) -> str:
    """ return permissions_use if it exists, else "creator_administrative_unit, else none """
    part_of = parameters_json.get('partOf', '')
    while isinstance(part_of, list):
        part_of = part_of[0]
    if "zp38w953h0s" in part_of or parameters_json.get('id', '') == 'zp38w953h0s': # Commencement Programs        
        return 'Permission to publish or publicly disseminate reproductions of any material obtained from the University Archives must be secured from the University Archives and any additional copyright owners prior to such use. Please see <a href="http://archives.nd.edu/about/useform.pdf">Conditions Governing Reproduction and Use of Material</a> for additional information'
    return parameters_json.get('permissions_use', parameters_json.get('creator_administrative_unit', None))


def _pick_repository(parameters_json: dict) -> str:
    """ return ARCHT for Architectural Lantern Slides, else UNDA """
    part_of = parameters_json.get('partOf', '')
    while isinstance(part_of, list):
        part_of = part_of[0]
    if parameters_json.get('collectionId', '') == "qz20sq9094h" or "qz20sq9094h" in part_of:  # Architectural Lantern Slides return ARCHT
        return 'ARCHT'
    return 'UNDA'


def _translate_work_type(parameters_json: dict) -> str:
    """ translates workType based on level and value of "hasModel" (workType) """
    part_of = parameters_json.get('partOf', '')
    while isinstance(part_of, list):
        part_of = part_of[0]
    if "zp38w953h0s" in part_of and parameters_json.get('hasModel', '') == 'Document': # Commencement Programs
        return 'program'
    if parameters_json.get('level', '') == 'manifest' and parameters_json.get('hasModel', '') == 'Image':
        return 'photographs'
    if parameters_json.get('hasModel', '') == 'LibraryCollection':
        return 'Collection'
    return parameters_json.get('hasModel', '')


def define_manifest_level(items: list) -> str:
    """ A collection has manifest items.  If the current node does not
        have manifest items, it is a manifest. (A manifest items can have only file items)"""
    level = "manifest"
    if len(items) > 0:
        for item in items:
            if item.get("level", "") == "manifest":
                level = "collection"
                break
    return level


def get_seed_nodes_json(json_node: dict, seed_nodes_control: dict or list) -> dict:
    """ We need to seed some json sections for extract_fields.
        This seeds those nodes as needed. """
    seed_json_output = {}
    if isinstance(seed_nodes_control, dict) or isinstance(seed_nodes_control, list):
        for node in seed_nodes_control:
            for key, value in node.items():
                if value in json_node:
                    seed_json_output[key] = json_node[value]
    return seed_json_output
