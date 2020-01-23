import json
import time
from datetime import datetime, timedelta
import requests
import os
import io
import boto3
from xml.etree import ElementTree
from create_json_from_xml_xpath import createJsonFromXml
from file_system_utilities import create_directory, get_full_path_file_name, copy_file_from_s3_to_local, \
    copy_file_from_local_to_s3, delete_file


class HarvestOaiEads():
    """ This performs all EAD-related processing """
    def __init__(self, config, event):
        self.config = config
        if 'eadToResourceDictionary' not in event:
            event['eadToResourceDictionary'] = {}
        self.event = event
        self.json_control = self._read_xml_to_json_translation_control_file("./xml_to_json_translation_control_file.json")  # noqa: E501
        self.base_oai_url = self.config['archive-space-server-base-url']
        self.start_time = time.time()
        self.jsonFromXMLClass = createJsonFromXml(self.config, self.json_control)
        self.eadToResourceDictFilename = self.config['process-bucket-ead-resource-mappings-file']
        self.repositories_of_interest = [3]
        s3 = boto3.resource('s3')
        self.bucket = s3.Bucket(self.config['process-bucket'])
        self.save_xml_locally = False
        self.json_locally = False
        self.temporary_local_path = '/tmp'

    def harvest_relevant_eads(self, mode, resumption_token):
        """ run appropriate logic given mode (full, incremental, ids, identifiers, known)"""
        if mode == 'full':
            resumption_token = self.full_oai_harvest(resumption_token)
        elif mode == 'incremental':
            change_date = datetime.now() - timedelta(hours=self.config['hours-threshold-for-incremental-harvest'])
            resumption_token = self.incremental_oai_harvest(resumption_token, change_date)
        elif mode == 'ids':
            resumption_token = self.ids_oai_harvest(resumption_token)
        elif mode == 'identifiers':
            ids = self.event.get("ids")
            while len(ids) > 0:
                resumption_token = ids[0]
                self._get_individual_ead(ids[0])
                del ids[0]
            if len(ids) == 0:
                resumption_token = None
        elif mode == 'known':
            ids = self.event.get("ids")
            ead_to_resource_dictionary = self._read_ead_to_resource_dictionary()
            for _key, value in ead_to_resource_dictionary.items():
                self._get_individual_ead(value)
            resumption_token = None
        return resumption_token

    def full_oai_harvest(self, resumption_token):
        """ Loop through each identifier, retain those in repositories we care about,
            and for each in interesting repositories, get oai_ead and create json """
        if resumption_token == "":
            self.event['eadToResourceDictionary'] = {}
        while resumption_token is not None:
            if int(time.time() - self.start_time) > (self.config['seconds-to-allow-for-processing']):
                break
            identifier, repository, resumption_token, xml_tree = self._get_next_records_data(resumption_token,
                                                                                             "",
                                                                                             "full")
            if repository in self.repositories_of_interest:
                self._get_individual_ead(identifier)

            print("identifier = ", identifier, repository, int(time.time() - self.start_time), 'seconds.')
        if resumption_token is None:
            self._save_ead_to_resource_dictionary()
        return resumption_token

    def incremental_oai_harvest(self, resumption_token, change_date):
        """ Loop through each identifier, retain those in repositories we care about,
            and for each in interesting repositories, get oai_ead and create json """
        while resumption_token is not None:
            if (time.time() - self.start_time) > (self.config['seconds-to-allow-for-processing']):
                break
            identifier, repository, resumption_token, xml_tree = self._get_next_records_data(resumption_token,
                                                                                             change_date,
                                                                                             "incremental")
            if repository in self.repositories_of_interest:
                for record in xml_tree.findall('./ListRecords/record'):
                    self._process_record(identifier, record)
            print("identifier = ", identifier, repository, int(time.time() - self.start_time), 'seconds.')
        return resumption_token

    def ids_oai_harvest(self, resumption_token):
        """ Harvest a list of ids defined in config['ids'].
            We need to do a lookup in our ead_to_resource_dictionary to find the ArchivesSpace identifier.
            We then use that identifier to query ArchivesSpace. """
        ids = self.event.get("ids")
        ead_to_resource_dictionary = self._read_ead_to_resource_dictionary()
        while len(ids) > 0:
            resumption_token = ids[0]
            identifier = ""
            if resumption_token in ead_to_resource_dictionary:
                identifier = ead_to_resource_dictionary[resumption_token]
            self._get_individual_ead(identifier)
            del ids[0]
        if len(ids) == 0:
            resumption_token = None
        return resumption_token

    def _read_ead_to_resource_dictionary(self):
        """ Retrieve ead_to_resource_dictionary.json """
        ead_to_resource_dictionary = {}
        file_name = self.eadToResourceDictFilename
        fully_qualified_file_name = get_full_path_file_name(self.temporary_local_path, file_name)
        if copy_file_from_s3_to_local(self.bucket, file_name, self.temporary_local_path, file_name):
            if os.path.exists(fully_qualified_file_name):
                with io.open(fully_qualified_file_name, 'r', encoding='utf-8') as json_file:
                    ead_to_resource_dictionary = json.load(json_file)
        return ead_to_resource_dictionary

    def _save_ead_to_resource_dictionary(self):
        """ Save ead_to_resource_dictionary.json """
        file_name = self.eadToResourceDictFilename
        fully_qualified_file_name = get_full_path_file_name(self.temporary_local_path, file_name)
        # print('saving dictionary ', self.event['eadToResourceDictionary'])
        with open(fully_qualified_file_name, 'w') as f:
            json.dump(self.event['eadToResourceDictionary'], f, indent=2)
        if copy_file_from_local_to_s3(self.bucket, file_name, self.temporary_local_path, file_name):
            delete_file(self.temporary_local_path, file_name)

    def _strip_namespaces(self, xml_string):
        """ In order to simplify xml harvest, we must strip these namespaces """
        namespaces_to_strip = ["ns4:", "ns3:", "ns2:", "ns1:", "ns0:",
                               "xlink:", "xmlns=\"http://www.openarchives.org/OAI/2.0/\"",
                               "xmlns=\"urn:isbn:1-931666-22-9\"",
                               "xmlns:ns1=\"http://www.w3.org/1999/xlink\"",
                               "xmlns:xlink=\"http://www.w3.org/1999/xlink\""]
        for string in namespaces_to_strip:
            xml_string = xml_string.replace(string, "")
        return xml_string

    def _get_next_url(self, verb="ListRecords", resumption_token="", from_date=""):
        """ Get the next URL given a resumption_token """
        url = self.base_oai_url + '?verb=' + verb
        if resumption_token == "":
            url += '&metadataPrefix=oai_ead'
            if verb == "ListRecords" and from_date != "":
                url += '&from=' + str(from_date.astimezone().isoformat())
        else:
            url += '&resumptionToken=' + resumption_token
        return url

    def _get_individual_ead(self, identifier):
        """ Retrieve one EAD xml record given the ArchivesSpace identifier """
        url = self._get_ead_url(identifier)
        json_summary = {}
        try:
            xml_string = self._strip_namespaces(requests.get(url, timeout=800).text)
        except ConnectionError:
            print("ConnectionError calling " + url)  # eventually want to write to sentry and continue
            return json_summary
        try:
            tree = ElementTree.fromstring(xml_string)
        except ElementTree.ParseError:
            print("Error retrieving " + identifier)
            print("xml_string = ", xml_string)  # eventually want to write to sentry and continue
            return json_summary
        records = tree.find('./GetRecord')
        if records is not None:
            for record in records.findall('./record'):
                json_summary = self._process_record(identifier, record)
        return json_summary

    def _process_record(self, identifier, record):
        """ Call a process to create JSON (and then CSV) output from complex ArchivesSpace EAD xml """
        json_summary = {}
        if self._digital_records_exist(record):
            ead_id = self._get_ead_id(record)
            json_summary = self.jsonFromXMLClass.extract_fields(record, 'root', {})
            if json_summary != {}:
                ead_id = self._get_ead_id(record)
                self.event['eadToResourceDictionary'][ead_id] = identifier
                # print(ead_id, ' = ', self.event['eadToResourceDictionary'][ead_id])
                print("identifier = ", identifier, int(time.time() - self.start_time), 'seconds.')
            if self.save_xml_locally:
                local_xml_output_folder = "/tmp/ead/xml/"
                create_directory(local_xml_output_folder)
                with open(local_xml_output_folder + ead_id + '.xml', 'w') as f:
                    f.write(ElementTree.tostring(record, encoding='unicode'))
            if self.json_locally:
                local_xml_output_folder = "/tmp/ead/json/"
                create_directory(local_xml_output_folder)
                with open(local_xml_output_folder + ead_id + '.json', 'w') as f:
                    f.write(ElementTree.tostring(record, encoding='unicode'))
        return json_summary

    def _get_ead_id(self, record):
        """ Retrieve EAD id from an ArchivesSpace record.  We will use this to identify this intellectual object."""
        ead_id = ""
        for node in record.findall('./metadata/ead/eadheader/eadid'):
            ead_id = node.text
            break
        return ead_id

    def _get_ead_url(self, identifier):
        """ Define the ArchivesSpace URL to retrive a given identifier."""
        url = self.base_oai_url + '?verb=GetRecord&identifier=' + identifier + '&metadataPrefix=oai_ead'
        return url

    def _get_resumption_token(self, xml_root, resumption_token_xpath):
        """ Read resumption token from xml """
        resumption_token = None
        resumption_xml = xml_root.find(resumption_token_xpath)
        if resumption_xml is not None:
            resumption_token = resumption_xml.text
        return resumption_token

    def _get_next_records_data(self, resumption_token, change_date, mode):
        """ Given resumption_token, get the data for the next record """
        if mode == 'full':
            oai_verb = "ListIdentifiers"
            identifier_xpath = "./ListIdentifiers/header/identifier"
        else:
            oai_verb = "ListRecords"
            identifier_xpath = "./ListRecords/record/header/identifier"
        resumption_token_xpath = "./" + oai_verb + "/resumptionToken"
        url = self._get_next_url(oai_verb, resumption_token, change_date)
        xml_string = self._strip_namespaces(requests.get(url).text)
        xml_tree = ElementTree.fromstring(xml_string)
        next_resumption_token = self._get_resumption_token(xml_tree, resumption_token_xpath)
        id_node = xml_tree.find(identifier_xpath)
        identifier = ''
        repository = 0
        if id_node is not None:
            identifier = id_node.text
            repository = int(identifier.split("/")[3])
        return identifier, repository, next_resumption_token, xml_tree

    def _read_xml_to_json_translation_control_file(self, filename):
        """ Read json file which defines xml to json translation """
        try:
            with open(filename, 'r') as input_source:
                data = json.load(input_source)
            input_source.close()
        except IOError:
            print('Cannot open ' + filename)
            raise
        return data

    def _digital_records_exist(self, xml_root):
        """ Test to see if a digital asset object record exists in the object """
        result = False
        for _dao in xml_root.findall('.//daogrp'):
            result = True
            break
        return result
