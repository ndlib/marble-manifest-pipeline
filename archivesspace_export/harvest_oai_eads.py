import json
import time
from datetime import datetime, timedelta
from xml.etree import ElementTree
from create_json_from_xml import createJsonFromXml
from file_system_utilities import create_directory
import requests  # noqa: E402
from dependencies.sentry_sdk import capture_exception


class HarvestOaiEads():
    """ This performs all EAD-related processing """
    def __init__(self, config: dict, event: dict):
        self.config = config
        self.event = event
        self.base_oai_url = self.config['archive-space-server-base-url']
        self.start_time = time.time()
        print("Will break after ", datetime.now() + timedelta(seconds=self.config['seconds-to-allow-for-processing']))
        self.jsonFromXMLClass = createJsonFromXml()
        self.save_xml_locally = True
        self.json_locally = True
        self.temporary_local_path = '/tmp'
        self.require_dao_flag = False

    def get_nd_json_from_archives_space_url(self, id_url: str) -> dict:
        """ Retrieve one EAD xml record given the ArchivesSpace identifier """
        oai_url = self._get_oai_url_given_id_url(id_url)
        nd_json = {}
        xml_string = self._get_xml_string_given_oai_url(oai_url)
        if xml_string:
            xml_tree = self._get_xml_tree_given_xml_string(xml_string, id_url)
            xml_record = xml_tree.find('./GetRecord/record')
            nd_json = self._process_record(oai_url, xml_record)
        return nd_json

    def _get_oai_url_given_id_url(self, user_interface_url: str) -> str:
        """ Define the ArchivesSpace URL to retrive a given identifier.
        user will pass: https://archivesspace.library.nd.edu/repositories/3/resources/1644
        we return: https://archivesspace.library.nd.edu/oai?verb=GetRecord&identifier=oai:und//repositories/3/resources/1644&metadataPrefix=oai_ead"""
        url = user_interface_url.replace("/repositories", "/oai?verb=GetRecord&identifier=oai:und//repositories") + "&metadataPrefix=oai_ead"
        return url

    def _get_xml_string_given_oai_url(self, oai_url: str) -> str:
        """ Given the oai url, return xml string, stripped of it's namespace information """
        try:
            xml_string = requests.get(oai_url, timeout=60).text
            xml_string = self._strip_namespaces(xml_string)
        except ConnectionError:
            print("ConnectionError calling " + oai_url)  # eventually want to write to sentry and continue
        return xml_string

    def _strip_namespaces(self, xml_string: str) -> str:
        """ In order to simplify xml harvest, we must strip these namespaces """
        namespaces_to_strip = ["ns4:", "ns3:", "ns2:", "ns1:", "ns0:",
                               "xlink:", "xmlns=\"http://www.openarchives.org/OAI/2.0/\"",
                               "xmlns=\"urn:isbn:1-931666-22-9\"",
                               "xmlns:ns1=\"http://www.w3.org/1999/xlink\"",
                               "xmlns:xlink=\"http://www.w3.org/1999/xlink\""]
        for string in namespaces_to_strip:
            xml_string = xml_string.replace(string, "")
        return xml_string

    def _get_xml_tree_given_xml_string(self, xml_string: str, id_url: str) -> ElementTree:
        """ translate the xml string into an ElementTree object for further use """
        xml_tree = ElementTree.fromstring("<empty/>")
        try:
            xml_tree = ElementTree.fromstring(xml_string)
        except ElementTree.ParseError as e:
            print("Error converting to xml results of this url: " + id_url)
            capture_exception(e)
        return xml_tree

    def _process_record(self, source_system_url: str, xml_record: ElementTree) -> dict:
        """ Call a process to create ND.JSON from complex ArchivesSpace EAD xml """
        nd_json = {}
        if not self.require_dao_flag or self._digital_records_exist(xml_record):
            ead_id = self._get_ead_id(xml_record)
            nd_json = self.jsonFromXMLClass.get_nd_json_from_xml(xml_record)
            if nd_json:
                print("ArchivesSpace ead_id = ", ead_id, " source_system_url = ", source_system_url, int(time.time() - self.start_time), 'seconds.')
            if self.save_xml_locally:
                local_xml_output_folder = "/tmp/ead/xml/"
                create_directory(local_xml_output_folder)
                with open(local_xml_output_folder + ead_id + '.xml', 'w') as f:
                    f.write(ElementTree.tostring(xml_record, encoding='unicode'))
            if self.json_locally:
                local_xml_output_folder = "/tmp/ead/json/"
                create_directory(local_xml_output_folder)
                with open(local_xml_output_folder + ead_id + '.json', 'w') as f:
                    json.dump(nd_json, f, indent=2)
        return nd_json

    def _get_ead_id(self, xml_record: ElementTree) -> str:
        """ Retrieve EAD id from an ArchivesSpace record.  We will use this to identify this intellectual object."""
        ead_id = ""
        ead_id = xml_record.find('./metadata/ead/eadheader/eadid').text
        return ead_id

    def _digital_records_exist(self, xml_root):
        """ Test to see if a digital asset object record exists in the object """
        result = False
        for _dao in xml_root.findall('.//daogrp'):
            result = True
            break
        return result
