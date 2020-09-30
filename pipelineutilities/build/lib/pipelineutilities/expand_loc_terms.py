# expand_subject_terms.py
""" Expand Library of Congress Subject Heading (LCSH) terms """
import json
import requests
from sentry_sdk import capture_exception
from validate_json import validate_json, get_subject_json_schema


def expand_loc_terms(subject: dict, depth: int = 1) -> dict:
    """ Expand Library of Congress (LOC) Subject Heading terms (LCSH) """
    if 'authority' not in subject:
        subject['authority'] = 'LCSH'
    api_url = _get_api_url(subject.get('uri', ''))
    if api_url:
        loc_json = _get_json_given_url(api_url + '.json')
        if loc_json:
            loc_item_node = _get_loc_item_node(loc_json, api_url)
            if 'term' not in subject:
                subject['term'] = _get_str_value(loc_item_node, 'http://www.loc.gov/mads/rdf/v1#authoritativeLabel')
            variant_links = loc_item_node.get("http://www.loc.gov/mads/rdf/v1#hasVariant", [])
            if variant_links:
                subject['variants'] = _get_loc_variants(loc_json, variant_links)
            for each_broader_authority in loc_item_node.get("http://www.loc.gov/mads/rdf/v1#hasBroaderAuthority", []):
                broader_uri = each_broader_authority.get('@id', '')
                if depth < 2 and not broader_uri.startswith('_:'):  # Note: "urls" starting with "_:" define authority organizations, not terms
                    if 'broaderTerms' not in subject:
                        subject['broaderTerms'] = []
                    this_new_subject = {"uri": broader_uri}
                    subject['broaderTerms'].append(expand_loc_terms(this_new_subject, depth + 1))
            if not validate_json(subject, get_subject_json_schema(), True):
                subject = None
    return subject


def _get_api_url(human_url: str) -> str:
    """ To get api url, use unsecured http, strip ".html" """
    return_url = human_url.replace('https://', 'http://').replace('.html', '')
    if return_url:
        return return_url  # Note:  This value will need ".json" appended, but since we must match key without .json, we append it separately
    return None


def _get_loc_variants(loc_json: dict, variant_links: list) -> list:
    """ Get Variations on the Term """
    variants = []
    for link in variant_links:
        key_to_find = link['@id']
        variant_node = _get_loc_item_node(loc_json, key_to_find)
        if variant_node:
            variants.append(_get_str_value(variant_node, 'http://www.loc.gov/mads/rdf/v1#variantLabel'))
    return variants


def _get_str_value(loc_json: dict, key_to_find: str, language: str = 'en') -> str:
    """ Return a string value associated with key_to_find['@value'] given a key_to_find and language """
    for item in loc_json.get(key_to_find, []):
        # Note:  "http://id.loc.gov/authorities includes @language for given nodes BUT http://id.loc.gov/vocabulary does not for those same nodes
        if item.get('@language', language) == language:
            return item['@value']
    return ''


def _get_loc_item_node(loc_json: dict, key_to_find: str) -> dict:
    """ Return the item node of interest within the LOC results """
    for node in loc_json:
        if node.get('@id', '') == key_to_find:
            return node
    return {}


def _get_json_given_url(url: str) -> dict:
    """ Return json from URL."""
    json_response = {}
    try:
        json_response = json.loads(requests.get(url).text)
    except ConnectionRefusedError as e:
        print('Connection refused on url ', url)
        capture_exception(e)
    except Exception as e:  # noqa E722 - intentionally ignore warning about bare except
        print('Error caught in expand_loc_terms._get_json_given_url trying to process url ' + url)
        capture_exception(e)
    return json_response


# python -c 'from expand_loc_terms import *; test()'
def test():
    """ test exection """
    subject_json = {
        "term": "Batting(Baseball)",
        "uri": "http://id.loc.gov/authorities/subjects/sh85012415",
    }
    subject_json = {
        "term": "Piano music, Arranged.",
        "uri": "http://id.loc.gov/authorities/subjects/sh85101791",
        "authority": "LCSH"
    }
    subject_json = {
        "term": "Prints.",
        "uri": "http://id.loc.gov/authorities/subjects/sh85106831",
        "authority": "LCSH",
    }

    resulting_json = expand_loc_terms(subject_json)
    print("Final output = ", resulting_json)
    # with open('resulting_output.json', 'w') as output_file:
    #     json.dump(resulting_json, output_file, indent=2)
