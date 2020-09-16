# expand_getty_ia_terms.py
""" Expand Getty Iconographic Authority (IA) terms (which are a sub-set of CONA terms) """
from xml.etree import ElementTree
import json  # pylint: disable=unused-import
import requests
from sentry_sdk import capture_exception
from validate_json import validate_json, get_subject_json_schema


def expand_ia_terms(subject: dict) -> dict:
    """ Expand IA terms """
    # Information on getty web services can be found here https://www.getty.edu/research/tools/vocabularies/vocab_web_services.pdf
    # given  "http://vocab.getty.edu/page/ia/901000032"
    # queries http://vocabsservices.getty.edu/CONAService.asmx/CONAGetIconography?iconography_id=901000032
    if 'authority' not in subject:
        subject['authority'] = 'AAT'
    ia_id = _get_id_given_url(subject.get('uri', ''))
    if ia_id:
        url = 'http://vocabsservices.getty.edu/CONAService.asmx/CONAGetIconography?iconography_id=' + ia_id
        if url:
            xml = _get_xml_given_url(url)
            note = _get_note(xml)
            if note:
                subject['description'] = note
            broader_terms, parent_terms = _get_broader_terms_given_id(ia_id)
            if broader_terms:
                subject['broaderTerms'] = broader_terms
            if parent_terms:
                subject['parentTerms'] = parent_terms
            variant_terms, preferred_term = _get_variant_terms(xml)
            if variant_terms:
                subject['variants'] = variant_terms
            if 'term' not in subject and preferred_term:
                subject['term'] = preferred_term
            if not validate_json(subject, get_subject_json_schema(), True):
                subject = None
    return subject


def _get_id_given_url(original_url: str) -> str:
    """ Parse id from original URL """
    return original_url.replace('https://', 'http://').replace('http://vocab.getty.edu/page/ia/', '')


def _get_note(xml: ElementTree) -> str:
    """ Get note from xml tree """
    xpath = "./Iconography_Records/Iconography_Record/Descriptive_Note"
    for xml_item in xml.findall(xpath):
        return xml_item.text
    return ''


def _get_variant_terms(xml: ElementTree) -> (list, str):
    """ Get variant terms from xml tree """
    variant_terms = []
    preferred_term = ''
    xpath = './Iconography_Records/Iconography_Record/Names/Name'
    for xml_item in xml.findall(xpath):
        if 'English' in xml_item.find('./Term_Languages/Term_Language/Language').text:
            term = xml_item.find('./Name_Text').text
            term_type = xml_item.find('./Preferred').text
            if term_type == 'Preferred':
                preferred_term = term
            elif term_type == 'Variant':
                variant_terms.append(term)
    return variant_terms, preferred_term


def _get_broader_terms_given_id(ia_id: str) -> (list, list):
    """ Get broader terms by following a different api url """
    broader_terms = []
    parent_terms = []
    if ia_id:
        url = 'http://vocabsservices.getty.edu/CONAService.asmx/CONAGetParentHierarchyIcon?iconographyID=' + ia_id
        xml = _get_xml_given_url(url)
        broader_terms, parent_terms = _get_broader_terms_given_xml(xml)
    return broader_terms, parent_terms


def _get_broader_terms_given_xml(xml: ElementTree) -> (list, list):
    """ parse the broader terms from xml of preferred parent and non preferred parent"""
    broader_terms = []
    parent_terms = []
    xpaths = ['./Iconography/Parent_Hierarchies/Preferred_Parent/Parent_Record', './Iconography/Parent_Hierarchies/Non-Preferred_Parent/Parent_Record']
    for xpath in xpaths:
        broader_term, parent_term = _get_broader_term_tree(xml, xpath)
        if broader_term:
            broader_terms.extend(broader_term)
            if parent_term:
                parent_terms.append(parent_term)
    return broader_terms, parent_terms


def _get_broader_term_tree(xml: ElementTree, xpath: str) -> (list, str):
    """ lookup broader terms given xpath """
    broader_terms = []
    root_parent_term = ''
    broader_terms_dict = {}
    for xml_term in xml.findall(xpath):
        broader_term = {}
        broader_term['authority'] = 'IA'
        broader_term['term'] = xml_term.find('./Pref_Term').text
        broader_term['uri'] = xml_term.find('./Parent_Subject_ID').text
        parent_level = int(xml_term.find('./Parent_Level').text)
        broader_terms_dict[parent_level] = broader_term
        broader_terms.append(broader_term)
    broader_terms, root_parent_term = _append_parent_to_broader_terms(broader_terms_dict)
    return broader_terms, root_parent_term


def _append_parent_to_broader_terms(broader_terms_dict: dict) -> (list, str):
    """ Add parent_term to broader_term records given inverse order of list """
    broader_terms_list = []
    root_parent_term = ''
    for key, value in broader_terms_dict.items():
        if key < len(broader_terms_dict):
            parent_term = broader_terms_dict[key + 1].get('term', '')
            value['parentTerm'] = parent_term
            if key == 1:
                root_parent_term = parent_term
        broader_terms_list.append(value)
    return broader_terms_list, root_parent_term


def _get_xml_given_url(url: str) -> ElementTree:
    """ retrieve the xml from the url """
    xml_string = _get_xml_string_given_url(url)
    xml_string = _strip_namespaces(xml_string)
    xml = _get_xml_tree_given_xml_string(xml_string, url)
    return xml


def _get_xml_string_given_url(url) -> str:
    """ return xml string given url """
    try:
        xml_string = requests.get(url, timeout=60).text
    except ConnectionError as e:
        capture_exception(e)
        xml_string = ""
        print("ConnectionError calling " + url)
    return xml_string


def _strip_namespaces(xml_string: str) -> str:
    """ In order to simplify xml harvest, we must strip these namespaces """
    namespaces_to_strip = ["ns4:", "ns3:", "ns2:", "ns1:", "ns0:",
                           'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"',
                           'xsi:noNamespaceSchemaLocation="http://vocabsservices.getty.edu/Schemas/AAT/AATGetSubject.xsd"',
                           'xsi:noNamespaceSchemaLocation="http://vocabsservices.getty.edu/Schemas/CONA/CONAGetIconography.xsd"',
                           'xsi:noNamespaceSchemaLocation="http://vocabsservices.getty.edu/Schemas/CONA/CONAGetParentHierarchyIcon.xsd"',
                           ]
    for string in namespaces_to_strip:
        xml_string = xml_string.replace(string, "")
    return xml_string


def _get_xml_tree_given_xml_string(xml_string: str, id_url: str) -> ElementTree:
    """ translate the xml string into an ElementTree object for further use """
    xml_tree = ElementTree.fromstring("<empty/>")
    try:
        xml_tree = ElementTree.fromstring(xml_string)
    except ElementTree.ParseError as e:
        print("Error converting to xml results of this url: " + id_url)
        capture_exception(e)
    return xml_tree


# export SSM_KEY_BASE=/all/new-csv
# aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
# python -c 'from expand_getty_ia_terms import *; test()'

# testing:
# python 'run_all_tests.py'

def test():
    """ test exection """
    subject_json = {
        "authority": "IA",
        "term": "Blessed Virgin Mary",
        "uri": "http://vocab.getty.edu/page/ia/901000032",
    }
    resulting_json = expand_ia_terms(subject_json)
    print("Final output = ", resulting_json)
    # with open('resulting_output.json', 'w') as output_file:
    #     json.dump(resulting_json, output_file, indent=2)
