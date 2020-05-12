# test_create_json_from_xml.py
""" test create_json_from_xml """
import _set_path  # noqa
import os
import json
import unittest
from create_json_from_xml import createJsonFromXml
import xml.etree.ElementTree as ET


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        self.xml = '<dsc> \
            <c01 id="aspace_8f5be6708c2e98e57a60eddf20e36679" level="file"> \
                <did> \
                    <unittitle>Theophilus Parsons, Journal, vol. 1</unittitle> \
                    <unitid>MSN/EA 8011-1-B</unitid> \
                    <unitdate type="inclusive">January 1819-December 1820</unitdate> \
                    <container id="aspace_65a928ca1f9c4e74da33694221550898" label="Mixed Materials" type="Box">1</container> \
                    <container id="aspace_a79fe3f27d1c7e6e97e59131e9ddd19f" parent="aspace_65a928ca1f9c4e74da33694221550898" type="Folder">1</container> \
                    <daogrp title="Theophilus Parsons, Journal, vol. 1" type="extended"> \
                        <daodesc> \
                            <p>Theophilus Parsons, Journal, vol. 1</p> \
                        </daodesc> \
                        <daoloc audience="external" href="https://rarebooks.library.nd.edu/digital/bookreader/MSN-EA_8011-1-B/?back=true&amp;2up=false#page/1/mode/1up" title="Theophilus Parsons, Journal, vol. 1" type="locator" /> \
                        <daoloc audience="external" href="https://rarebooks.library.nd.edu/digital/bookreader/MSN-EA_8011-1-B/images/MSN-EA_8011-01-B-000a.jpg" title="Theophilus Parsons, Journal, vol. 1" type="locator" /> \
                    </daogrp> \
                </did> \
            </c01> \
            <c01 id="aspace_5cc53e5bc083823390135923b389d00c" level="file"> \
                <did> \
                    <unittitle>Theophilus Parsons, Journal, vol. 2</unittitle> \
                    <unitid>MSN/EA 8011-2-B</unitid> \
                    <unitdate type="inclusive">January 1821-March 1823</unitdate> \
                    <container id="aspace_21e36ac4728edf05982900bcf99b61e3" label="Mixed Materials" type="Box">1</container> \
                    <container id="aspace_9c22e3676e6077a217d56bcd205f2d65" parent="aspace_21e36ac4728edf05982900bcf99b61e3" type="Folder">2</container> \
                    <daogrp title="Theophilius Parsons, Journal, vol. 2" type="extended"> \
                        <daodesc> \
                            <p>Theophilius Parsons, Journal, vol. 2</p> \
                        </daodesc> \
                        <daoloc audience="external" href="https://rarebooks.library.nd.edu/digital/bookreader/MSN-EA_8011-2-B/?back=true&amp;2up=false#page/1/mode/1up" title="Theophilius Parsons, Journal, vol. 2" type="locator" /> \
                    <daoloc audience="external" href="https://rarebooks.library.nd.edu/digital/bookreader/MSN-EA_8011-2-B/images/MSN-EA_8011-02-B-000a.jpg" title="Theophilius Parsons, Journal, vol. 2" type="locator" /> \
                    </daogrp> \
                </did> \
            </c01> \
        </dsc>'

    def test_1_test_creating_json_from_xml(self):
        """ test test_creating_json_from_xml """
        xml_record = ET.parse(local_folder + 'test/MSNEA8011_EAD.xml').getroot()
        create_json_from_xml_class = createJsonFromXml()
        nd_json = create_json_from_xml_class.get_nd_json_from_xml(xml_record)
        with open(local_folder + 'test/MSNEA8011_EAD.json', 'r') as input_source:
            expected_json = json.load(input_source)
        nd_json = self.fix_file_created_date(nd_json, expected_json["fileCreatedDate"])
        # with open(local_folder + 'test/actual_MSNEA8011_EAD.json', 'w') as output_source:
        #     json.dump(nd_json, output_source, indent=2)
        self.assertTrue(expected_json == nd_json)

    def test_2_test_extracting_id(self):
        xml_record = ET.fromstring(self.xml)
        create_json_from_xml_class = createJsonFromXml()
        expected_results = ["aspace_8f5be6708c2e98e57a60eddf20e36679", "aspace_5cc53e5bc083823390135923b389d00c"]
        for index, xml_item in enumerate(xml_record.findall('./c01')):
            field = {'label': 'id', 'xpath': '.', 'optional': True, 'returnAttributeName': 'id', 'format': 'text'}
            actual_results = create_json_from_xml_class._get_node(xml_item, field, {})
            self.assertTrue(expected_results[index] == actual_results)

    def test_3_test_extracting_items(self):
        xml_record = ET.fromstring(self.xml)
        create_json_from_xml_class = createJsonFromXml()
        for index, xml_item in enumerate(xml_record.findall('./c01')):
            actual_results = create_json_from_xml_class.extract_fields(xml_item, "items", {})
            expected_results = {'id': 'aspace_8f5be6708c2e98e57a60eddf20e36679', 'title': 'Theophilus Parsons, Journal, vol. 1', 'dateCreated': 'January 1819-December 1820', 'uniqueIdentifier': 'MSN/EA 8011-1-B', 'items': [{'level': 'file', 'thumbnail': True, 'description': 'Theophilus Parsons, Journal, vol. 1', 'filePath': 'https://rarebooks.library.nd.edu/digital/bookreader/MSN-EA_8011-1-B/images/MSN-EA_8011-01-B-000a.jpg', 'parentId': 'aspace_8f5be6708c2e98e57a60eddf20e36679', 'id': 'MSN-EA_8011-01-B-000a.jpg'}], 'level': 'manifest'}
            self.assertTrue(expected_results == actual_results)
            break

    def fix_file_created_date(self, nd_json: dict, file_created_date: str) -> dict:
        if "fileCreatedDate" in nd_json:
            nd_json["fileCreatedDate"] = file_created_date
        if "items" in nd_json:
            for item in nd_json["items"]:
                item = self.fix_file_created_date(item, file_created_date)
        return nd_json


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
