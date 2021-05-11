# do_extra_processing.py
import os
import json
import datetime


def do_extra_processing(value: str, extra_processing: str) -> str:
    """ If extra processing is required, make appropriate calls to perform that additional processing. """
    results = ""
    if extra_processing == "link_to_source":
        results = "https://onesearch.library.nd.edu/permalink/f/1phik6l/ndu_aleph" + value
    elif extra_processing == "format_subjects":
        results = _format_subjects(value)
    elif extra_processing == "format_creators":
        results = _format_creators(value)
    elif extra_processing == "translate_repository":
        results = _translate_repository(value)
    elif extra_processing == "format_call_number":
        results = _format_call_number(value)
    elif extra_processing == "format_collections":
        results = _format_collections(value)
    elif extra_processing == "format_manually_modified_date":
        results = _format_manually_modified_date(value)
    elif extra_processing == "find_latest_date_batch_modified_date":
        results = _find_latest_date_batch_modified_date(value)
    return results


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


def _format_call_number(value: str) -> str:
    """ Call numbers have a space between sub-fields h and i.  """
    results = ""
    if "^^^" in value:
        results = value.split("^^^")[0] + " " + value.split("^^^")[1]
    else:
        results = value
    return results


def _translate_repository(value: str) -> str:
    """ Translate repository to known desired list.  If not in hash, use existing value """
    local_folder = os.path.dirname(os.path.realpath(__file__))
    with open(local_folder + '/repository_mapping.json', 'r') as input_source:
        repository_mapping_json = json.load(input_source)
    if value.upper() in repository_mapping_json:
        value = repository_mapping_json[value.upper()]
    return value


def _format_collections(value: list) -> dict:
    """ Add display to collections.  """
    results = []
    for each_value in value:
        node = {"display": each_value.replace(" (University of Notre Dame. Library)", "")}
        results.append(node)
    return results


def _format_manually_modified_date(value: str) -> str:
    """ Date comes from Aleph looking like this: 20191121125041.0
        return 2019-11-21T12:50:41.0"""
    date_obj = datetime.datetime.strptime(value, '%Y%m%d%H%M%S.%f')
    return date_obj.isoformat()


def _find_latest_date_batch_modified_date(value: str) -> str:
    """ We will receive value like this: ['20191121 1240', '20191121 1241', '20191121 1243'] """
    latest_modified_date = ''
    for date_str in value:
        this_iso_date = datetime.datetime.strptime(date_str, '%Y%m%d %H%M').isoformat()
        if this_iso_date > latest_modified_date:
            latest_modified_date = this_iso_date
    return latest_modified_date
