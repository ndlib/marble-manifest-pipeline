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
from dynamo_save_functions import save_new_subject_term_authority_record, save_unharvested_subject_term_record, save_subject_term_record
from dynamo_query_functions import get_subject_term_record
import random
from datetime import datetime, timedelta

days_before_expanding_terms_again = 30


def get_expanded_subject_terms(standard_json: dict, dynamo_table_name: str = None) -> dict:  # noqa:C901
    """ For each term, check to see if it is already expanded in dynamo
        If so, see if we need to expand terms again.
        If we don't need to expand terms again, just use the already expanded term from dynamo """
    for subject in standard_json.get('subjects', []):
        if subject.get('term', ''):
            subject['display'] = subject['term']
        subject_authority = _define_subject_authority(subject.get('authority', ''), subject.get('uri', ''))
        subject['authority'] = subject_authority
        uri = subject.get('uri')
        if uri:
            if subject_authority not in ['', 'AAT', 'FAST', 'IA', 'LCSH', 'LOCAL']:
                save_new_subject_term_authority_record(dynamo_table_name, subject_authority, standard_json.get('sourceSystem', ''), standard_json.get('id'))
                save_unharvested_subject_term_record(dynamo_table_name, subject_authority, standard_json.get('sourceSystem', ''), subject.get('term'), standard_json.get('id'), subject.get('uri'))
            record_from_dynamo = get_subject_term_record(dynamo_table_name, uri)

            if record_from_dynamo:
                for k, v in record_from_dynamo.items():
                    if k not in ('PK', 'SK', 'GSI1PK', 'GSI1SK', 'GSI2PK', 'GSI2SK', 'TYPE', 'dateAddedToDynamo', 'dateModifiedInDynamo'):
                        subject[k] = v

            if not record_from_dynamo:
                save_subject_term_record(dynamo_table_name, subject.copy(), False, True)
    return standard_json


def expand_subject_terms_recursive(standard_json: dict, dynamo_table_name: str = None) -> dict:
    """ Traverse entire standard_json tree, expanding subject terms at every level """
    if 'subjects' in standard_json:
        standard_json = get_expanded_subject_terms(standard_json, dynamo_table_name)
    for item in standard_json.get('items', []):
        item = expand_subject_terms_recursive(item, dynamo_table_name)
    return standard_json


def expand_subject_term(subject: dict, dynamo_table_name: str = None) -> dict:
    """ Expand term for various authorities """
    if not subject.get('uri'):
        return subject
    subject['dateModifiedInDynamo'] = _get_last_modified_date_to_save(subject.get('dateModifiedInDynamo'))
    need_to_save_expanded_subject_term = False
    authority = subject.get('authority', 'X')

    if authority == 'LCSH':
        expand_loc_terms(subject)
        need_to_save_expanded_subject_term = True
    elif authority == 'IA':
        expand_ia_terms(subject)
        need_to_save_expanded_subject_term = True
    elif authority == 'AAT':
        expand_aat_terms(subject)
        need_to_save_expanded_subject_term = True
    elif authority in ('FAST') or '/id.worldcat.org/fast/' in subject.get('uri', ''):
        #  Note:  The api url here:  http://id.worldcat.org/fast/1000579/rdf.xml for "uri": "http://id.worldcat.org/fast/01000579" does not seem helpful
        pass  # intentionally skip harvesting fast, since content there doesn't seem to add value
    elif authority in ('LOCAL'):
        pass  # we can't expand Local authority
    elif subject.get('uri', ''):
        print('unable to expand subject uri in ', subject)

    if subject.get('term', ''):
        subject['display'] = subject['term']

    subject = _add_display_to_broader_terms(subject)

    if need_to_save_expanded_subject_term:
        save_subject_term_record(dynamo_table_name, subject, True, False)
    return subject


def _define_subject_authority(authority: str, uri: str) -> str:
    authority = authority.upper()
    if (len(authority) and authority in ('LCSH', 'LOC', 'LIBRARY OF CONGRESS SUBJECT HEADINGS')) or '/id.loc.gov/' in uri:
        authority = 'LCSH'
    elif (len(authority) and authority in ('IA')) or '/vocab.getty.edu/page/ia/' in uri:
        authority = 'IA'
    elif (len(authority) and authority in ('AAT')) or '/vocab.getty.edu/aat/' in uri:
        authority = 'AAT'
    elif (len(authority) and authority in ('FAST')) or '/id.worldcat.org/fast/' in uri:
        authority = 'FAST'
    elif (len(authority) and authority in ('LOCAL')):
        authority = 'LOCAL'
    return authority


def _add_display_to_broader_terms(subject: dict) -> dict:
    for broader_term in subject.get('broaderTerms', []):
        if broader_term.get('term', ''):
            broader_term['display'] = broader_term['term']
    return subject


def _get_last_modified_date_to_save(date_last_modified_in_dynamo: str) -> str:
    """ If we have never expanded this term before, set random date within the last 30 days.  Otherwise, set as today """
    days = _get_offset_days(date_last_modified_in_dynamo)
    last_modified_date = datetime.now() - timedelta(days=days)
    return last_modified_date.isoformat()


def _get_offset_days(date_last_modified_in_dynamo: str) -> int:
    """ If we have never expanded this term before (no dateLastModifiedInDynamo),
    return a random number between 1 and 30.
    If we have expanded this term before, return 0. """
    days = 0
    if not date_last_modified_in_dynamo:
        days = random.randint(1, days_before_expanding_terms_again)
    return days


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
    for subject in seed_json.get('subjects', []):
        resulting_json = expand_subject_term(subject)
        print("Final output = ", resulting_json)
    # with open('resulting_output.json', 'w') as output_file:
    #     json.dump(resulting_json, output_file, indent=2)
