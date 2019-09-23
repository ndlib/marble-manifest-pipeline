# xml_processing.py
""" This module includes simple functions for interacting with xml """
import xml.etree.ElementTree as ET


def get_image_list_from_metadata_file(file_name):
    tree = ET.parse(file_name)
    root = tree.getroot()
    image_file_list = get_image_list_from_metadata_xml(root)
    return image_file_list


def get_image_list_from_metadata_xml(xml):
    """ Get an array of image files from metadata xml """
    image_file_list = []
    image_div_path = '{http://www.loc.gov/METS/}structMap/{http://www.loc.gov/METS/}div/{http://www.loc.gov/METS/}div'
    for item in xml.findall(image_div_path):
        file_node = {}
        # file_node['label'] = item.find('.').attrib['LABEL']
        file_node['order'] = item.find('.').attrib['ORDER']
        # file_node['fileId'] = item.find('{http://www.loc.gov/METS/}fptr').attrib['FILEID']
        file_id = item.find('{http://www.loc.gov/METS/}fptr').attrib['FILEID']
        file_node['fileName'] = file_id.replace('ID_', '')
        file_node['filePath'] = get_file_path_given_xml_and_mets_file_id(xml, file_id)
        image_file_list.append(file_node)
    return image_file_list


def get_file_path_given_xml_and_mets_file_id(xml, mets_file_id):
    file_path = ''
    xpath = "{http://www.loc.gov/METS/}fileSec/{http://www.loc.gov/METS/}fileGrp/{http://www.loc.gov/METS/}file[@ID='" + mets_file_id + "']"  # noqa: E501
    for item in xml.findall(xpath):
        file_path = item.find('{http://www.loc.gov/METS/}FLocat').attrib['{http://www.w3.org/1999/xlink}href']
        file_path = file_path.replace('GOOGLE::Snite Archive-Collection Team Drive::/Media/images/', '')
    return file_path
