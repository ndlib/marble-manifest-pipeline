# do_extra_processing.py
import os
import json


def do_extra_processing(value: str, extra_processing: str) -> str:
    """ If extra processing is required, make appropriate calls to perform that additional processing. """
    results = ""
    if extra_processing == "link_to_source":
        results = "https://onesearch.library.nd.edu/primo-explore/fulldisplay?docid=ndu_aleph" + value + "&context=L&vid=NDU&lang=en_US&search_scope=malc_blended&adaptor=Local%20Search%20Engine&tab=onesearch&query=any,contains,ndu_aleph002097132&mode=basic"  # noqa: E501
    elif extra_processing == "lookup_work_type":
        results = _lookup_work_type(value)
    elif extra_processing == "format_subjects":
        results = _format_subjects(value)
    elif extra_processing == "format_creators":
        results = _format_creators(value)
    elif extra_processing == "translate_repository":
        results = _translate_repository(value)
    return results


def _lookup_work_type(key_to_find: str) -> str:
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


def _format_subjects(value: list) -> dict:
    """ Subjects require special formatting.  """
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


def _format_creators(value: list) -> dict:
    """ Creators require special formatting.
        $a	Personal name (ex: Griesinger, Peggy)
        $b	Numeration (ex. III) (used for royalty or people who are Jrs. or Srs.)
        $c	Titles and other words associated with a name (ex. King of England)
        $d	Dates associated with a name (ex. 1989-2089) this is birth and death date, second date blank if the person is still alive
        $q Fuller form of name (ex. Librarian) - this is used as a way to differentiate between people with very similar names who also share birthdays - it happens!
        """
    results = []
    for each_value in value:
        node = {}
        node["attribution"] = ""
        # node["role"] = "Primary"
        if "^^^" in each_value:
            node["fullName"] = each_value.split("^^^")[0]
            node["lifeDates"] = each_value.split("^^^")[1]
        else:
            node['fullName'] = each_value
        node['display'] = node.get("fullName", "")
        results.append(node)
    return results


def _translate_repository(value: str) -> str:
    """ Translate repository to known desired list.  If not in hash, use existing value """
    local_folder = os.path.dirname(os.path.realpath(__file__))
    with open(local_folder + '/repository_mapping.json', 'r') as input_source:
        repository_mapping_json = json.load(input_source)
    if value.upper() in repository_mapping_json:
        value = repository_mapping_json[value.upper()]
    return value
