# modify_existing_index_record.py  4/4/19
""" Modifies existing pnx record to remove irrelevant info and replace repository  """

# import sys
import re
from xml.etree.ElementTree import ElementTree, Element  # , SubElement  # , tostring
# from get_thumbnail_from_manifest import get_thumbnail_from_manifest


def _create_xml_element(element_name, element_text):
    """ Create XML element including name and text """
    record = Element(element_name)
    if element_text is not None:
        if element_text > "":
            record.text = element_text
    return record


def modify_existing_index_record(pnx_xml, json_input):
    """ Modifies PNX record for re-insertion for use by Mellon"""
    repository = json_input['repository']
    library = json_input['library']
    library_collection_code = json_input['library_collection_code']
    display_library = json_input['display_library']
    # unique_identifier = json_input['id']
    thumbnail_url = ''
    if 'thumbnail' in json_input:
        thumbnail_url = json_input['thumbnail']
    pnx_xml = _strip_control_section_from_pnx(pnx_xml)
    pnx_xml = _strip_dedup_section_from_pnx(pnx_xml)
    pnx_xml = _strip_frbr_section_from_pnx(pnx_xml)
    _add_repository_to_display(pnx_xml, display_library)
    _add_repository_to_search(pnx_xml, repository, library_collection_code)
    _add_repository_to_facet(pnx_xml, library)
    _add_thumbnail_to_links(pnx_xml, thumbnail_url)
    final_new_pnx_xml = _enclose_pnx_in_new_root_node(pnx_xml, json_input)
    return(final_new_pnx_xml)


def _add_repository_to_display(pnx_xml, display_library):
    for display_section in pnx_xml.findall('display'):
        # remove existing nodes to be replaced
        # for lds10_section in display_section.findall('lds10'):
        #     display_section.remove(lds10_section)
        # add new node
        display_section.append(_create_xml_element('lds10', display_library.title()))
    return


def _add_repository_to_search(pnx_xml, repository, library_collection_code):
    # search_section = ElementTree.Element("search")
    for search_section in pnx_xml.findall('search'):
        # remove existing nodes to be replaced
        # for scope_node in search_section.findall('scope'):
        #     search_section.remove(scope_node)
        # for lsr01_node in search_section.findall('lsr01'):
        #     search_section.remove(lsr01_node)
        # add new nodes
        search_section.append(_create_xml_element('scope', repository.upper()))
        search_section.append(_create_xml_element('lsr01', library_collection_code.upper()))
    return


def _add_repository_to_facet(pnx_xml, library):
    for facet_section in pnx_xml.findall('facet'):
        # remove existing nodes to be replaced
        # for library_section in facet_section.findall('library'):
        #     facet_section.remove(library_section)
        # add new node
        facet_section.append(_create_xml_element('library', library.upper()))
    return


def _add_thumbnail_to_links(pnx_xml, thumbnail_url):
    # for items currently stored in Primo under "ndu_aleph", translate prefix to "ils-" to find manifest
    # unique_identifier = _correct_docid_for_manifest_search(unique_identifier)
    # url = get_thumbnail_from_manifest(unique_identifier)
    # if url > '':
    for links_section in pnx_xml.findall('links'):
        for thumbnail_section in links_section.findall('thumbnail'):
            if thumbnail_section.text.startswith('$$Uhttps://image-iiif.library.nd.edu'):
                links_section.remove(thumbnail_section)  # if a thumbnail to the image server exists, remove it
        links_section.append(_create_xml_element('thumbnail', '$$U' + thumbnail_url))
    return


def _correct_docid_for_manifest_search(docid):
    """ Correct Doc ID based on patterns """
    # for items currently stored in Primo under ndu_aleph, manifest prefix is "ils-".
    docid = re.sub('ndu_aleph', 'ils-', docid)  # If "ndu_aleph" is supplied, translate to "ils-" to find Manifest
    return docid


def _strip_control_section_from_pnx(pnx_xml):
    """ We must strip the control section from a PNX record before re-inserting it """
    for control_section in pnx_xml.findall('control'):
        pnx_xml.remove(control_section)
    return(pnx_xml)


def _strip_dedup_section_from_pnx(pnx_xml):
    """ We must strip the dedup section from a PNX record before re-inserting it """
    for dedup_section in pnx_xml.findall('dedup'):
        pnx_xml.remove(dedup_section)
    return(pnx_xml)


def _strip_frbr_section_from_pnx(pnx_xml):
    """ We must strip the frbr section from a PNX record before re-inserting it """
    for frbr_section in pnx_xml.findall('frbr'):
        pnx_xml.remove(frbr_section)
    return(pnx_xml)
#
#
# def get_unique_identifier_from_original_pnx(pnx_xml):
#     unique_identifier = ""
#     for control_section in pnx_xml.findall('control'):
#         for recordid_node in control_section.findall('recordid'):
#             unique_identifier = recordid_node.text
#     if unique_identifier == '':
#         for display_section in pnx_xml.findall('display'):
#             for lds02_node in display_section.findall('lds02'):
#                 unique_identifier = lds02_node.text
#     return unique_identifier


def _enclose_pnx_in_new_root_node(pnx_xml, json_input):
    root = Element("records")
    # root.append(_create_record_section(json_input))
    # unique_identifier = 'unique_identifier'
    # SubElement(pnx_xml, 'id').text = unique_identifier
    pnx_xml.append(_create_xml_element('id', _create_record_id(json_input)))
    root.append(pnx_xml)
    new_pnx_tree = ElementTree(root)
    return new_pnx_tree


def _create_record_id(json_input):
    """ Create Record Section of Primo Record """
    record_id = ""
    if 'manifest_url' in json_input:
        record_id = json_input['manifest_url']
    else:
        record_id = json_input['id']
    return record_id
