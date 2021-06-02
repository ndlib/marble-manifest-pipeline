""" test save_json_to_dynamo """

import _set_path  # noqa
import unittest
from pipelineutilities.dynamo_helpers import format_key_value, add_item_keys, add_file_group_keys, add_file_keys, \
    add_source_system_keys, add_item_to_harvest_keys, add_file_systems_keys, add_parent_override_keys, \
    add_file_to_process_keys, add_website_keys, \
    add_website_item_keys, add_new_subject_term_authority_keys, add_unharvested_subject_term_keys, add_subject_term_keys, \
    _add_more_file_fields, add_image_group_keys, add_image_keys, add_media_group_keys, add_media_keys


class Test(unittest.TestCase):
    """ Class for test fixtures """

    def test_01_format_key_value(self):
        """ test_01_format_key_value """
        key_value = 'a B c 1 2 3'
        actual_results = format_key_value(key_value)
        expected_results = 'ABC123'
        self.assertEqual(actual_results, expected_results)

    def test_02_add_item_keys(self):
        """ test_02_add_item_keys """
        standard_json = {'id': 'abc def', 'parentId': 'root', 'sequence': 1, 'sourceSystem': 's s 1', 'title': 'my title'}
        actual_results = add_item_keys(standard_json)
        expected_results = {
            'id': 'abc def', 'parentId': 'root', 'sequence': 1, 'sourceSystem': 's s 1', 'title': 'my title',
            'PK': 'ITEM#ABCDEF', 'SK': 'ITEM#ABCDEF', 'TYPE': 'Item',
            'GSI1PK': 'ITEM#ROOT', 'GSI1SK': 'SORT#00001#MYTITLE',
            'GSI2PK': 'SOURCESYSTEM#SS1', 'GSI2SK': 'SORT#MYTITLE'}
        del actual_results['dateModifiedInDynamo']
        self.assertEqual(actual_results, expected_results)

    def test_03_add_file_group_keys(self):
        """ test_03_add_file_group_keys """
        standard_json = {'objectFileGroupId': 'abc 123', 'storageSystem': 'S3', 'typeOfData': 'RBSC website bucket'}
        actual_results = add_file_group_keys(standard_json)
        expected_results = {
            'objectFileGroupId': 'abc 123', 'storageSystem': 'S3', 'typeOfData': 'RBSC website bucket',
            'PK': 'FILEGROUP', 'SK': 'FILEGROUP#ABC123', 'TYPE': 'FileGroup',
            'GSI1PK': 'FILEGROUP#ABC123', 'GSI1SK': '#NAME#ABC123',
            'GSI2PK': 'FILESYSTEM#S3#RBSCWEBSITEBUCKET', 'GSI2SK': 'FILEGROUP#ABC123'}
        del actual_results['dateAddedToDynamo']
        del actual_results['dateModifiedInDynamo']
        self.assertEqual(actual_results, expected_results)

    def test_04_add_file_keys(self):
        """ test_04_add_file_keys """
        standard_json = {'id': 'some id', 'objectFileGroupId': 'abc 123', 'sequence': 2}
        actual_results = add_file_keys(standard_json)
        expected_results = {
            'id': 'some id', 'objectFileGroupId': 'abc 123', 'sequence': 2,
            'PK': 'FILE', 'SK': 'FILE#SOMEID', 'TYPE': 'File',
            'GSI1PK': 'FILEGROUP#ABC123', 'GSI1SK': 'SORT#00002'}
        del actual_results['dateModifiedInDynamo']
        self.assertEqual(actual_results, expected_results)

    def test_05_add_source_system_keys(self):
        """ test_05_add_source_system_keys """
        source_system_record = {'sourceSystem': 'some source system'}
        actual_results = add_source_system_keys(source_system_record)
        expected_results = {
            'sourceSystem': 'some source system',
            'PK': 'SOURCESYSTEM', 'SK': 'SOURCESYSTEM#SOMESOURCESYSTEM', 'TYPE': 'SourceSystem',
            'GSI2PK': 'SOURCESYSTEM#SOMESOURCESYSTEM', 'GSI2SK': 'SOURCESYSTEM#SOMESOURCESYSTEM'}
        del actual_results['dateModifiedInDynamo']
        self.assertEqual(actual_results, expected_results)

    def test_06_add_item_to_harvest_keys(self):
        """ test_06_add_item_to_harvest_keys """
        json_record = {'sourceSystem': 'some system', 'harvestItemId': 'item to harvest id'}
        actual_results = add_item_to_harvest_keys(json_record)
        expected_results = {
            'sourceSystem': 'some system', 'harvestItemId': 'item to harvest id',
            'PK': 'ITEMTOHARVEST', 'SK': 'SOURCESYSTEM#SOMESYSTEM#ITEMTOHARVESTID', 'TYPE': 'ItemToHarvest'}
        del actual_results['dateAddedToDynamo']
        del actual_results['dateModifiedInDynamo']
        self.assertEqual(actual_results, expected_results)

    def test_07_add_file_systems_keys(self):
        """ test_07_add_file_systems_keys """
        json_record = {'storageSystem': 'S3', 'typeOfData': 'RBSC website bucket'}
        actual_results = add_file_systems_keys(json_record)
        expected_results = {
            'storageSystem': 'S3', 'typeOfData': 'RBSC website bucket',
            'PK': 'FILESYSTEM', 'SK': 'FILESYSTEM#S3#RBSCWEBSITEBUCKET', 'TYPE': 'FileSystem'}
        del actual_results['dateAddedToDynamo']
        del actual_results['dateModifiedInDynamo']
        self.assertEqual(actual_results, expected_results)

    def test_08_add_parent_override_keys(self):
        """ test_08_add_parent_override_keys """
        json_record = {'id': 'some id', 'parentId': 'dad'}
        actual_results = add_parent_override_keys(json_record)
        expected_results = {
            'id': 'some id', 'parentId': 'dad',
            'PK': 'ITEM#SOMEID', 'SK': 'PARENT#DAD', 'TYPE': 'ParentOverride',
            'GSI1PK': 'PARENTOVERRIDE', 'GSI1SK': 'ITEM#SOMEID'
        }
        del actual_results['dateAddedToDynamo']
        del actual_results['dateModifiedInDynamo']
        self.assertEqual(actual_results, expected_results)

    def test_09_add_file_to_process_keys(self):
        """ test_09_add_file_to_process_keys """
        json_record = {'filePath': 'https://drive.google.com/a/nd.edu/file/d/1ZGi24M2EeCR9PiYfW_sCPYQcZmTM0dWZ/view', 'parentId': 'dad'}
        json_record['storageSystem'] = 'Google'
        json_record['typeOfData'] = 'Museum'
        actual_results = add_file_to_process_keys(json_record)
        expected_results = {
            'filePath': 'https://drive.google.com/a/nd.edu/file/d/1ZGi24M2EeCR9PiYfW_sCPYQcZmTM0dWZ/view', 'parentId': 'dad', 'storageSystem': 'Google', 'typeOfData': 'Museum',
            'PK': 'FILETOPROCESS', 'SK': 'FILEPATH#HTTPS://DRIVE.GOOGLE.COM/A/ND.EDU/FILE/D/1ZGI24M2EECR9PIYFW_SCPYQCZMTM0DWZ/VIEW',
            'TYPE': 'FileToProcess',
            'GSI1PK': 'FILETOPROCESS', 'GSI1SK': 'FILESYSTEM#GOOGLE#MUSEUM',
            'GSI2PK': 'FILETOPROCESS', 'GSI2SK': 'DATELASTPROCESSED#'
        }
        del actual_results['dateAddedToDynamo']
        del actual_results['dateModifiedInDynamo']
        self.assertEqual(actual_results, expected_results)

    def test_10_add_website_keys(self):
        """ test_10_add_website_keys """
        json_record = {'id': 'marble'}
        actual_results = add_website_keys(json_record)
        expected_results = {
            'id': 'marble',
            'PK': 'WEBSITE', 'SK': 'WEBSITE#MARBLE',
            'TYPE': 'WebSite',
            'GSI1PK': 'WEBSITE#MARBLE', 'GSI1SK': 'WEBSITE#MARBLE'
        }
        del actual_results['dateAddedToDynamo']
        del actual_results['dateModifiedInDynamo']
        self.assertEqual(actual_results, expected_results)

    def test_13_add_website_item_term_keys(self):
        """ test_13_add_website_item_term_keys """
        json_record = {'id': 'abc123', 'websiteId': 'my website'}
        actual_results = add_website_item_keys(json_record)
        expected_results = {
            'id': 'abc123', 'websiteId': 'my website',
            'PK': 'WEBSITE#MYWEBSITE', 'SK': 'ITEM#ABC123',
            'TYPE': 'WebsiteItem',
            'itemId': 'abc123',
            'GSI1PK': 'WEBSITE#MYWEBSITE'
        }
        del actual_results['dateAddedToDynamo']
        actual_results.pop('dateModifiedInDynamo', None)
        del actual_results['GSI1SK']
        self.assertEqual(actual_results, expected_results)

    def test_14_add_new_subject_term_authority_keys(self):
        """ test_14_add_new_subject_term_authority_keys """
        json_record = {'authority': 'newauthority', 'sourceSystem': 'some source'}
        actual_results = add_new_subject_term_authority_keys(json_record)
        expected_results = {
            'authority': 'newauthority', 'sourceSystem': 'some source',
            'PK': 'NEWSUBJECTTERMAUTHORITY', 'SK': 'AUTHORITY#NEWAUTHORITY',
            'TYPE': 'NewSubjectTermAuthority',
            'GSI1PK': 'NEWSUBJECTTERMAUTHORITY', 'GSI1SK': 'SOURCESYSTEM#SOMESOURCE'
        }
        del actual_results['dateAddedToDynamo']
        actual_results.pop('dateModifiedInDynamo', None)
        self.assertEqual(actual_results, expected_results)

    def test_15_add_unharvested_subject_term_keys(self):
        """ test_15_add_unharvested_subject_term_keys """
        json_record = {'term': 'new term', 'authority': 'newauthority', 'sourceSystem': 'some source'}
        actual_results = add_unharvested_subject_term_keys(json_record)
        expected_results = {
            'term': 'new term', 'authority': 'newauthority', 'sourceSystem': 'some source',
            'PK': 'UNHARVESTEDSUBJECTTERM', 'SK': 'TERM#NEWTERM',
            'TYPE': 'UnharvestedSubjectTerm',
            'GSI1PK': 'UNHARVESTEDSUBJECTTERM', 'GSI1SK': 'AUTHORITY#NEWAUTHORITY'
        }
        del actual_results['dateAddedToDynamo']
        actual_results.pop('dateModifiedInDynamo', None)
        self.assertEqual(actual_results, expected_results)

    def test_16_add_subject_term_keys(self):
        """ test_16_add_subject_term_keys for insert"""
        json_record = {'uri': 'some uri', 'authority': 'some authority'}
        actual_results = add_subject_term_keys(json_record)
        expected_results = {
            'uri': 'some uri', 'authority': 'some authority',
            'PK': 'SUBJECTTERM', 'SK': 'URI#SOMEURI',
            'TYPE': 'SubjectTerm',
            'GSI1PK': 'SUBJECTTERM', 'GSI1SK': 'AUTHORITY#SOMEAUTHORITY#',
            'GSI2PK': 'SUBJECTTERM', 'GSI2SK': 'LASTHARVESTDATE#'}
        del actual_results['dateAddedToDynamo']
        actual_results.pop('dateModifiedInDynamo', None)
        self.assertEqual(actual_results, expected_results)

    def test_17_add_subject_term_keys(self):
        """ test_17_add_subject_term_keys for update """
        json_record = {'uri': 'some uri', 'authority': 'some authority'}
        actual_results = add_subject_term_keys(json_record, True)
        expected_results = {
            'uri': 'some uri', 'authority': 'some authority',
            'PK': 'SUBJECTTERM', 'SK': 'URI#SOMEURI',
            'TYPE': 'SubjectTerm',
            'GSI1PK': 'SUBJECTTERM', 'GSI1SK': 'AUTHORITY#SOMEAUTHORITY#',
            'GSI2PK': 'SUBJECTTERM', 'GSI2SK': 'LASTHARVESTDATE#'}
        expected_results['GSI1SK'] = expected_results.get('GSI1SK') + actual_results['dateModifiedInDynamo']
        expected_results['GSI2SK'] = expected_results.get('GSI2SK') + actual_results['dateModifiedInDynamo']
        del actual_results['dateAddedToDynamo']
        actual_results.pop('dateModifiedInDynamo', None)
        self.assertEqual(actual_results, expected_results)

    def test_18_add_more_file_fields(self):
        """ test_18_add_more_file_fields"""
        json_record = {"filePath": "collections/ead_xml/images/MSN-EA_5031/MSN-EA_5031-01.a.150.tif"}
        actual_results = _add_more_file_fields(json_record, 'http://SomeIiifServeruri')
        expected_results = {
            'filePath': 'collections/ead_xml/images/MSN-EA_5031/MSN-EA_5031-01.a.150.tif',
            'mediaServer': 'http://SomeIiifServeruri',
            'mimeType': 'image/tiff', 'mediaResourceId': 'collections%2Fead_xml%2Fimages%2FMSN-EA_5031%2FMSN-EA_5031-01.a.150'
        }
        self.assertEqual(actual_results, expected_results)

    def test_19_add_more_file_fields(self):
        """ test_18_add_more_file_fields"""
        json_record = {"filePath": "collections/ead_xml/images/MSN-EA_5031/MSN-EA_5031-01.a.150.pdf"}
        actual_results = _add_more_file_fields(json_record, 'http://SomeIiifServeruri')
        expected_results = {
            'filePath': 'collections/ead_xml/images/MSN-EA_5031/MSN-EA_5031-01.a.150.pdf',
            'mimeType': 'application/pdf'
        }
        self.assertEqual(actual_results, expected_results)

    def test_20_add_image_group_keys(self):
        """ test_20_add_image_group_keys """
        standard_json = {'imageGroupId': 'abc 123', 'storageSystem': 'S3', 'typeOfData': 'RBSC website bucket'}
        actual_results = add_image_group_keys(standard_json)
        expected_results = {
            'imageGroupId': 'abc 123', 'storageSystem': 'S3', 'typeOfData': 'RBSC website bucket',
            'PK': 'IMAGEGROUP', 'SK': 'IMAGEGROUP#ABC123', 'TYPE': 'ImageGroup',
            'GSI1PK': 'IMAGEGROUP#ABC123', 'GSI1SK': '#NAME#ABC123',
            'GSI2PK': 'FILESYSTEM#S3#RBSCWEBSITEBUCKET', 'GSI2SK': 'IMAGEGROUP#ABC123'}
        del actual_results['dateAddedToDynamo']
        del actual_results['dateModifiedInDynamo']
        self.assertEqual(actual_results, expected_results)

    def test_21_add_image_keys(self):
        """ test_21_add_image_keys """
        standard_json = {'id': 'some id', 'imageGroupId': 'abc 123', 'sequence': 2}
        actual_results = add_image_keys(standard_json)
        expected_results = {
            'id': 'some id', 'imageGroupId': 'abc 123', 'sequence': 2,
            'PK': 'IMAGE', 'SK': 'IMAGE#SOMEID', 'TYPE': 'Image',
            'GSI1PK': 'IMAGEGROUP#ABC123', 'GSI1SK': 'SORT#00002'}
        del actual_results['dateModifiedInDynamo']
        self.assertEqual(actual_results, expected_results)

    def test_22_add_media_group_keys(self):
        """ test_22_add_media_group_keys """
        standard_json = {'mediaGroupId': 'abc 123', 'storageSystem': 'S3', 'typeOfData': 'RBSC website bucket'}
        actual_results = add_media_group_keys(standard_json)
        expected_results = {
            'mediaGroupId': 'abc 123', 'storageSystem': 'S3', 'typeOfData': 'RBSC website bucket',
            'PK': 'MEDIAGROUP', 'SK': 'MEDIAGROUP#ABC123', 'TYPE': 'MediaGroup',
            'GSI1PK': 'MEDIAGROUP#ABC123', 'GSI1SK': '#NAME#ABC123',
            'GSI2PK': 'FILESYSTEM#S3#RBSCWEBSITEBUCKET', 'GSI2SK': 'MEDIAGROUP#ABC123'}
        del actual_results['dateAddedToDynamo']
        del actual_results['dateModifiedInDynamo']
        self.assertEqual(actual_results, expected_results)

    def test_23_add_media_keys(self):
        """ test_23_add_media_keys """
        standard_json = {'id': 'some id', 'mediaGroupId': 'abc 123', 'sequence': 2}
        actual_results = add_media_keys(standard_json)
        expected_results = {
            'id': 'some id', 'mediaGroupId': 'abc 123', 'sequence': 2,
            'PK': 'MEDIA', 'SK': 'MEDIA#SOMEID', 'TYPE': 'Media',
            'GSI1PK': 'MEDIAGROUP#ABC123', 'GSI1SK': 'SORT#00002'}
        del actual_results['dateModifiedInDynamo']
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    # save_data_for_tests()
    suite()
    unittest.main()
