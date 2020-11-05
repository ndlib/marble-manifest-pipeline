# expand_getty_aat_terms.py
""" Expand Getty Art and Architecture Thesaurus (AAT) terms """
from xml.etree import ElementTree
import json  # noqa: F401
import re
import requests
from sentry_sdk import capture_exception
from validate_json import validate_json, get_subject_json_schema


def expand_aat_terms(subject: dict) -> dict:
    """ Expand AAT terms """
    # Information on getty web services can be found here https://www.getty.edu/research/tools/vocabularies/vocab_web_services.pdf
    if subject.get('authority', '') not in ('AAT') and '/vocab.getty.edu/aat/' not in subject.get('uri', ''):
        return None
    if 'authority' not in subject:
        subject['authority'] = 'AAT'
    api_url = _get_api_url(subject.get('uri', ''))
    if api_url:
        aat_xml = _get_xml_given_url(api_url)
        note = _get_note(aat_xml)
        if note:
            subject['description'] = note
        broader_terms, parent_terms = _get_broader_terms(aat_xml)
        if broader_terms:
            subject['broaderTerms'] = broader_terms
        if parent_terms:
            subject['parentTerms'] = parent_terms
        variants = _get_variants(aat_xml)
        if variants:
            subject['variants'] = variants
        if not validate_json(subject, get_subject_json_schema(), True):
            subject = None
    return subject


def _get_api_url(human_url: str) -> str:
    """ Get API url """
    return human_url.replace('vocab.getty.edu/aat/', 'vocabsservices.getty.edu/AATService.asmx/AATGetSubject?subjectID=')


def _get_note(xml: ElementTree) -> str:
    """ Get note text if any """
    xpath = "./Subject/Descriptive_Notes/Descriptive_Note"
    for xml_item in xml.findall(xpath):
        language_node = xml_item.find("./Note_Language")
        if language_node is not None:
            if language_node.text == 'English':
                note_node = xml_item.find("./Note_Text")
                if note_node is not None:
                    value_found = note_node.text
                    return value_found
    return ''


def _get_broader_terms(xml: ElementTree) -> (list, list):
    """ Get braoder terms if any """
    broader_terms = []
    parent_terms = []
    xpaths = ['./Subject/Parent_Relationships/Preferred_Parent', './Subject/Parent_Relationships/Non-Preferred_Parent']
    for xpath in xpaths:
        broader_term = _get_each_broader_term(xml, xpath)
        if broader_term:
            broader_terms.extend(broader_term)
            parent_term = broader_term[0].get("term", '')
            if parent_term:
                parent_terms.append(parent_term)
    return broader_terms, parent_terms


def _get_each_broader_term(xml: ElementTree, xpath: str) -> list:
    """ Get either preferred or non-preferred parent hierarchy tree, based on xpath, and return as broader terms. """
    broader_terms = []
    xml_term = xml.find(xpath)
    if xml_term is not None:
        node = xml_term.find("./Relationship_Type")
        if node is not None:
            if node.text == "Parent/Child":
                parent_node = xml_term.find('Parent_String')
                if parent_node is not None:
                    parent_string = parent_node.text
                    if parent_string:
                        broader_terms = _parse_parent_string_into_broader_terms(parent_string)
    return broader_terms


def _parse_parent_string_into_broader_terms(parent_string: str) -> list:
    """ Given the string of the parent hierarchy, return list of broader terms """
    break_on_terms = ["styles, periods, and cultures by region"]
    broader_terms = []
    parent_list = parent_string.split("],")
    for i, list_item in enumerate(parent_list):
        list_item += ']'  # add back in bracket we removed as part of the split        term = _get_term_from_string(list_item)
        term = _get_term_from_string(list_item)
        uri = _get_uri_from_string(list_item)
        if 'hierarchy name' in term or term in break_on_terms:
            break
        broader_term = {}
        broader_term['authority'] = 'AAT'
        broader_term['term'] = _get_term_from_string(list_item)
        if uri:
            broader_term['uri'] = uri
        if i < len(parent_list) - 1:
            parent_term = _get_term_from_string(parent_list[i + 1] + ']')
            if 'hierarchy name' not in parent_term and parent_term not in break_on_terms:
                broader_term['parentTerm'] = parent_term
        broader_terms.append(broader_term)
    return broader_terms


def _get_term_from_string(term_string: str) -> str:
    """ remove literal pattern of [0-9] from string to return remaining as the term """
    return re.sub(r'\[[0-9]*\]', '', term_string).strip()


def _get_uri_from_string(term_string: str) -> str:
    """ build the url as literal plus only the numbers within [] from the term string """
    if '[' in term_string:
        return 'http://vocab.getty.edu/aat/' + term_string[term_string.index('['):].replace('[', '').replace(']', '')
    return ''


def _get_xml_given_url(url: str) -> ElementTree:
    """ retrieve the xml from the url """
    xml_string = _get_xml_string_given_url(url)
    xml_string = _strip_namespaces(xml_string)
    xml = _get_xml_tree_given_xml_string(xml_string, url)
    return xml


def _get_xml_string_given_url(url) -> str:
    """ return xml string given url """
    xml_string = ""
    try:
        xml_string = requests.get(url, timeout=60).text
    except ConnectionError as e:
        capture_exception(e)
        print("ConnectionError calling " + url)
    except TimeoutError as e:
        capture_exception(e)
        print("TimeoutError calling " + url)
    except Exception as e:
        capture_exception(e)
        print(str(e) + " Error calling " + url)
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


def _get_variants(xml: ElementTree) -> list:
    """ Get braoder terms if any """
    variant_terms = []
    xpaths = ['./Subject/Terms/Non-Preferred_Term']
    for xpath in xpaths:
        for xml_term in xml.findall(xpath):
            language = xml_term.find('./Term_Languages/Term_Language/Language')
            if language is not None:
                if 'English' in language.text:
                    variant_term = xml_term.find('./Term_Text')
                    if variant_term is not None:
                        variant_terms.append(variant_term.text)
    return variant_terms


# testing:
# python -c 'from expand_getty_aat_terms import *; test()'
def test():
    """ test exection """
    seed_json = {
        "authority": "AAT",
        "term": "Benin",
        "uri": "http://vocab.getty.edu/aat/300015777",
    }
    resulting_json = expand_aat_terms(seed_json)
    print("Final output = ", resulting_json)
    # with open('resulting_output.json', 'w') as output_file:
    #     json.dump(resulting_json, output_file, indent=2)
