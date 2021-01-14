""" test save_json_to_dynamo """
import _set_path  # noqa
import unittest
from pipelineutilities.dynamo_helpers import format_key_value, add_item_keys, add_file_group_keys, add_file_keys, add_source_system_keys


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
            'PK': 'ITEM#ABCDEF', 'SK': 'ITEM#ABCDEF', 'type': 'Item',
            'GSI1PK': 'ITEM#ROOT', 'GSI1SK': 'SORT#00001#MYTITLE',
            'GSI2PK': 'SOURCESYSTEM#SS1', 'GSI2SK': 'SORT#MYTITLE'}
        del actual_results['dateModifiedInDynamo']
        self.assertEqual(actual_results, expected_results)

    def test_03_add_file_group_keys(self):
        """ test_03_add_file_group_keys """
        standard_json = {'objectFileGroupId': 'abc 123'}
        actual_results = add_file_group_keys(standard_json)
        expected_results = {
            'objectFileGroupId': 'abc 123',
            'PK': 'FILEGROUP', 'SK': 'FILEGROUP#ABC123', 'type': 'FileGroup',
            'GSI1PK': 'FILEGROUP#ABC123', 'GSI1SK': '#NAME#ABC123'}
        del actual_results['dateModifiedInDynamo']
        self.assertEqual(actual_results, expected_results)

    def test_04_add_file_keys(self):
        """ test_04_add_file_keys """
        standard_json = {'id': 'some id', 'objectFileGroupId': 'abc 123', 'sequence': 2}
        actual_results = add_file_keys(standard_json)
        expected_results = {
            'id': 'some id', 'objectFileGroupId': 'abc 123', 'sequence': 2,
            'PK': 'FILE', 'SK': 'FILE#SOMEID', 'type': 'File',
            'GSI1PK': 'FILEGROUP#ABC123', 'GSI1SK': 'SORT#00002'}
        del actual_results['dateModifiedInDynamo']
        self.assertEqual(actual_results, expected_results)

    def test_05_add_source_system_keys(self):
        """ test_05_add_source_system_keys """
        source_system_record = {'sourceSystem': 'some source system'}
        actual_results = add_source_system_keys(source_system_record)
        expected_results = {
            'sourceSystem': 'some source system',
            'PK': 'SOURCESYSTEM', 'SK': 'SOURCESYSTEM#SOMESOURCESYSTEM', 'type': 'SourceSystem',
            'GSI2PK': 'SOURCESYSTEM#SOMESOURCESYSTEM', 'GSI2SK': 'SOURCESYSTEM#SOMESOURCESYSTEM'}
        del actual_results['dateModifiedInDynamo']
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    # save_data_for_tests()
    suite()
    unittest.main()
