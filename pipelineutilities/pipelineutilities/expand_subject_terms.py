"""
Expand Subject Terms
Currently expands subject terms for these dictionaries:
  LOC - Library of Congress
  LCSH - Library of Congress Subject Headings
  IA - Getty Iconographic Authority
  AAT - Getty Art and Architecture Thesaurus
Note:  we are currently intentionally not expanding FAST terms, since those don't seem to add value

"""
import json  # noqa: F401
from expand_loc_terms import expand_loc_terms
from expand_getty_ia_terms import expand_ia_terms
from expand_getty_aat_terms import expand_aat_terms


def expand_subject_terms(standard_json: dict) -> dict:  # noqa: C901
    """ Expand terms for various authorities """
    if 'subjects' in standard_json:
        for subject in standard_json.get('subjects', []):
            authority = subject.get('authority', 'x')
            authority = authority.upper()
            if authority != 'X':
                subject['authority'] = authority
            if authority in ('LCSH', 'LOC') or '/id.loc.gov/' in subject.get('uri', ''):
                expand_loc_terms(subject)
            elif authority in ('IA') or '/vocab.getty.edu/page/ia/' in subject.get('uri', ''):
                expand_ia_terms(subject)
            elif authority in ('AAT') or '/vocab.getty.edu/aat/' in subject.get('uri', ''):
                expand_aat_terms(subject)
            elif authority in ('FAST') or '/id.worldcat.org/fast/' in subject.get('uri', ''):
                #  Note:  The api url here:  http://id.worldcat.org/fast/1000579/rdf.xml for "uri": "http://id.worldcat.org/fast/01000579" does not seem helpful
                pass  # intentionally skip harvesting fast, since content there doesn't seem to add value
            elif authority not in ('X'):
                print("unknown authority in ", subject)
            elif subject.get('uri', ''):
                print('unable to expand subject uri in ', subject)

            if subject.get('term', ''):
                subject['display'] = subject['term']
            subject = _add_display_to_broader_terms(subject)
    return standard_json


def expand_subject_terms_recursive(standard_json: dict) -> dict:
    """ Traverse entire standard_json tree, expanding subject terms at every level """
    if 'subjects' in standard_json:
        expand_subject_terms(standard_json)
    for item in standard_json.get('items', []):
        item = expand_subject_terms_recursive(item)
    return standard_json


def _add_display_to_broader_terms(subject: dict) -> dict:
    for broader_term in subject.get('broaderTerms', []):
        if broader_term.get('term', ''):
            broader_term['display'] = broader_term['term']
    return subject


def _init_json():
    seed_json = {
        "subjects": [
            # {
            #     "authority": "IA",
            #     "term": "Blessed Virgin Mary",
            #     "uri": "http://vocab.getty.edu/page/ia/901000032",
            # },
            # {
            #     "authority": "AAT",
            #     "term": "crowns",
            #     "uri": "http://vocab.getty.edu/aat/300046020",
            # },
            # {
            #     "term": "North America Maps.",
            #     "uri": "http://id.loc.gov/authorities/subjects/sh2008116388",
            # },
            # {
            #     "term": "Batting(Baseball)",
            #     "uri": "http://id.loc.gov/authorities/subjects/sh85012415",
            # },
            # {"uri": "http://id.loc.gov/authorities/subjects/sh85011217"},
            # {
            #     "term": "Great Lakes Region (North America) Maps."
            # },
            # Note:  The api url is here:  http://id.worldcat.org/fast/1000579/rdf.xml
            # {
            #     "term": "Liturgics.",
            #     "uri": "http://id.worldcat.org/fast/01000579"
            # },
            {
                "authority": "AAT",
                "term": "Expressionist",
                "uri": "http://vocab.getty.edu/aat/300021502"
            }
        ]
    }
    return seed_json


# python -c 'from expand_subject_terms import *; test()'
def test():
    """ test exection """
    seed_json = _init_json()
    resulting_json = expand_subject_terms(seed_json)
    print("Final output = ", resulting_json)
    # with open('resulting_output.json', 'w') as output_file:
    #     json.dump(resulting_json, output_file, indent=2)
