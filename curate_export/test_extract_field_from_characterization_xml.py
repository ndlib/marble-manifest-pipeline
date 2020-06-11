# test_translate_curate_json_node.py
""" test translate_curate_json_node """
import _set_path  # noqa
import unittest
import os
from extract_field_from_characterization_xml import extract_field_from_characterization_xml, _strip_namespaces


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        self.characterization_xml_string = "<fits xmlns=\"http://hul.harvard.edu/ois/xml/ns/fits/fits_output\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://hul.harvard.edu/ois/xml/ns/fits/fits_output http://hul.harvard.edu/ois/xml/xsd/fits/fits_output.xsd\" version=\"0.6.2\" timestamp=\"2/2/20 10:06 PM\">\n  <identification>\n    <identity format=\"Portable Document Format\" mimetype=\"application/pdf\" toolname=\"FITS\" toolversion=\"0.6.2\">\n      <tool toolname=\"Jhove\" toolversion=\"1.5\"/>\n      <tool toolname=\"file utility\" toolversion=\"5.04\"/>\n      <tool toolname=\"Exiftool\" toolversion=\"9.06\"/>\n      <tool toolname=\"Droid\" toolversion=\"3.0\"/>\n      <tool toolname=\"NLNZ Metadata Extractor\" toolversion=\"3.4GA\"/>\n      <tool toolname=\"ffident\" toolversion=\"0.2\"/>\n      <version toolname=\"file utility\" toolversion=\"5.04\">1.6</version>\n      <externalIdentifier toolname=\"Droid\" toolversion=\"3.0\" type=\"puid\">fmt/20</externalIdentifier>\n    </identity>\n  </identification>\n  <fileinfo>\n    <size toolname=\"Jhove\" toolversion=\"1.5\">87442</size>\n    <creatingApplicationName toolname=\"Exiftool\" toolversion=\"9.06\" status=\"CONFLICT\">University of Notre Dame Archives/University of Notre Dame Archives</creatingApplicationName>\n    <creatingApplicationName toolname=\"NLNZ Metadata Extractor\" toolversion=\"3.4GA\" status=\"CONFLICT\">/</creatingApplicationName>\n    <lastmodified toolname=\"Exiftool\" toolversion=\"9.06\" status=\"SINGLE_RESULT\">2020:02:02 22:06:24-05:00</lastmodified>\n    <created toolname=\"Exiftool\" toolversion=\"9.06\" status=\"SINGLE_RESULT\">2015:08:04 14:17:05-04:00</created>\n    <filepath toolname=\"OIS File Information\" toolversion=\"0.1\" status=\"SINGLE_RESULT\">/tmp/und:pz50gt57r3h-content.1.pdf20200202-76701-1hqth20.pdf</filepath>\n    \n    <md5checksum toolname=\"OIS File Information\" toolversion=\"0.1\" status=\"SINGLE_RESULT\">e5ad298d464aa992c94e07c7b1b165f1</md5checksum>\n    <fslastmodified toolname=\"OIS File Information\" toolversion=\"0.1\" status=\"SINGLE_RESULT\">1580699184000</fslastmodified>\n  </fileinfo>\n  <filestatus>\n    <well-formed toolname=\"Jhove\" toolversion=\"1.5\" status=\"SINGLE_RESULT\">false</well-formed>\n    <valid toolname=\"Jhove\" toolversion=\"1.5\" status=\"SINGLE_RESULT\">false</valid>\n    <message toolname=\"Jhove\" toolversion=\"1.5\" status=\"SINGLE_RESULT\">Invalid object number in cross-reference stream offset=87381</message>\n  </filestatus>\n  <metadata>\n    <document>\n      <title toolname=\"Exiftool\" toolversion=\"9.06\" status=\"SINGLE_RESULT\">University of Notre Dame Commencement Program</title>\n      <pageCount toolname=\"Exiftool\" toolversion=\"9.06\" status=\"SINGLE_RESULT\">1</pageCount>\n      <isTagged toolname=\"Jhove\" toolversion=\"1.5\">no</isTagged>\n      <hasOutline toolname=\"Jhove\" toolversion=\"1.5\">no</hasOutline>\n      <hasAnnotations toolname=\"Jhove\" toolversion=\"1.5\" status=\"SINGLE_RESULT\">no</hasAnnotations>\n      <isRightsManaged toolname=\"Exiftool\" toolversion=\"9.06\" status=\"SINGLE_RESULT\">no</isRightsManaged>\n      <isProtected toolname=\"Exiftool\" toolversion=\"9.06\">no</isProtected>\n      <hasForms toolname=\"NLNZ Metadata Extractor\" toolversion=\"3.4GA\" status=\"SINGLE_RESULT\">no</hasForms>\n    </document>\n  </metadata>\n</fits>"  # noqa: E501

    def test_01_strip_namespaces(self):
        characterization_xml_string = "<something>text\n</something>"
        actual_results = _strip_namespaces(characterization_xml_string)
        expected_results = "<something>text</something>"
        self.assertTrue(actual_results == expected_results)
        characterization_xml_string = "<something xmlns=\"http://hul.harvard.edu/ois/xml/ns/fits/fits_output\">text\n</something>"
        actual_results = _strip_namespaces(characterization_xml_string)
        # print("actual_results = ", actual_results)
        expected_results = "<something >text</something>"
        self.assertTrue(actual_results == expected_results)

    def test_02_extract_field_from_characterization_xml(self):
        field_to_extract = "./fileinfo/md5checksum"
        actual_results = extract_field_from_characterization_xml(self.characterization_xml_string, field_to_extract)
        expected_results = "e5ad298d464aa992c94e07c7b1b165f1"
        self.assertTrue(actual_results == expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
