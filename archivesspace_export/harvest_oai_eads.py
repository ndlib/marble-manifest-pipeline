from xml.etree import ElementTree
from create_json_from_xml import createJsonFromXml
import requests  # noqa: E402
from dependencies.sentry_sdk import capture_exception
from dynamo_query_functions import get_item_record


class HarvestOaiEads():
    """ This performs all EAD-related processing """
    def __init__(self, config: dict):
        self.config = config
        self.base_oai_url = self.config['archive-space-server-base-url']
        self.jsonFromXMLClass = createJsonFromXml()
        self.temporary_local_path = '/tmp'
        self.require_dao_flag = False

    def get_standard_json_from_archives_space_url(self, id_url: str) -> dict:
        """ Retrieve one EAD xml record given the ArchivesSpace identifier """
        oai_url = self._get_oai_url_given_id_url(id_url)
        standard_json = {}
        xml_string = self._get_xml_string_given_oai_url(oai_url)
        if xml_string:
            xml_tree = self._get_xml_tree_given_xml_string(xml_string, id_url)
            xml_record = xml_tree.find('./GetRecord/record')
            date_modified_in_source_system = self._get_modified_date_from_xml_record(xml_record)
            item_id = self._get_id_from_xml_record(xml_record)
            date_modified_in_dynamo = self._get_modified_date_from_dynamo(item_id)
            if date_modified_in_source_system > date_modified_in_dynamo or self.config.get('forceSaveStandardJson', False):
                standard_json = self._process_record(oai_url, xml_record)
            else:
                print("Metadata current in DynamoDB, no need to reprocess ", item_id, ' from ', id_url)
        return standard_json

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
        except ConnectionError as e:
            capture_exception(e)
            xml_string = ""
            print("ConnectionError calling " + oai_url)
        except Exception as e:
            capture_exception(e)
            xml_string = ""
            print("Error calling " + oai_url)
        return xml_string

    def _strip_namespaces(self, xml_string: str) -> str:
        """ In order to simplify xml harvest, we must strip these namespaces """
        namespaces_to_strip = ["ns4:", "ns3:", "ns2:", "ns1:", "ns0:",
                               "xlink:", "xmlns=\"http://www.openarchives.org/OAI/2.0/\"",
                               "xmlns=\"urn:isbn:1-931666-22-9\"",
                               "xmlns:ns1=\"http://www.w3.org/1999/xlink\"",
                               "xmlns:xlink=\"http://www.w3.org/1999/xlink\"",
                               "<i>", "</i>", '<emph render="italic">', '</emph>']
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
        standard_json = self.jsonFromXMLClass.get_standard_json_from_xml(xml_record)
        return standard_json

    def _get_modified_date_from_xml_record(self, xml_record: ElementTree) -> str:
        """ Return modified date from xml_record """
        return xml_record.find("./header/datestamp").text

    def _get_id_from_xml_record(self, xml_record: ElementTree) -> str:
        """ Return Item Id from xml_record """
        return xml_record.find("./metadata/ead/eadheader/eadid").text

    def _get_modified_date_from_dynamo(self, item_id: str) -> str:
        """ Modified Date represents the date modified in the source system record stored in dynamo """
        record_from_dynamo = get_item_record(self.config.get('website-metadata-tablename'), item_id)
        return record_from_dynamo.get('modifiedDate', '')
