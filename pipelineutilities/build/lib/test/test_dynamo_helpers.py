""" test save_json_to_dynamo """
import _set_path  # noqa
import unittest
from pipelineutilities.dynamo_helpers import format_key_value, add_item_keys, add_file_group_keys, add_file_keys, \
    add_source_system_keys, add_item_to_harvest_keys, add_file_systems_keys, add_parent_override_keys, \
    add_file_to_process_keys, add_website_keys, add_subject_term_to_expand_keys, add_expanded_subject_term_keys


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
            'GSI1PK': 'FILETOPROCESS', 'GSI1SK': 'FILESYSTEM#GOOGLE#MUSEUM'
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

    def test_11_add_subject_term_to_expand_keys(self):
        """ test_11_aadd_subject_term_to_expand_keys """
        json_record = {'authority': 'AAT', 'uri': 'http://vocab.getty.edu/aat/300010662'}
        actual_results = add_subject_term_to_expand_keys(json_record)
        expected_results = {
            'authority': 'AAT', 'uri': 'http://vocab.getty.edu/aat/300010662',
            'PK': 'SUBJECTTERMTOEXPAND', 'SK': 'URI#HTTP://VOCAB.GETTY.EDU/AAT/300010662',
            'TYPE': 'SubjectTermToExpand',
            'GSI1PK': 'SUBJECTTERMTOEXPAND', 'GSI1SK': 'AUTHORITY#HTTP://VOCAB.GETTY.EDU/AAT/300010662'
        }
        del actual_results['dateAddedToDynamo']
        del actual_results['dateModifiedInDynamo']
        self.assertEqual(actual_results, expected_results)

    def test_12_add_expanded_subject_term_keys(self):
        """ test_12_add_expanded_subject_term_keys """
        json_record = {'uri': 'http://vocab.getty.edu/aat/300010662'}
        actual_results = add_expanded_subject_term_keys(json_record)
        expected_results = {
            'uri': 'http://vocab.getty.edu/aat/300010662',
            'PK': 'EXPANDEDSUBJECTTERM', 'SK': 'URI#HTTP://VOCAB.GETTY.EDU/AAT/300010662',
            'TYPE': 'ExpandedSubjectTerm'
        }
        del actual_results['dateAddedToDynamo']
        del actual_results['dateModifiedInDynamo']
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    # save_data_for_tests()
    suite()
    unittest.main()
