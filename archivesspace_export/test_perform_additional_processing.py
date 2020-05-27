# test_add_image_records_as_child_items.py
""" test add_image_records_as_child_items """
import unittest
from perform_additional_processing import perform_additional_processing, format_creators, \
    get_repository_name_from_ead_resource, define_manifest_level
from datetime import date


class Test(unittest.TestCase):
    """ Class for test fixtures """

    def test_1_format_creators(self):
        """ test_1 format_creators """
        passed_value = "something"
        returned_array = format_creators(passed_value)
        expected_array = [{'attribution': '', 'role': 'Primary', 'fullName': 'something', 'display': 'something'}]
        self.assertTrue(returned_array == expected_array)

    def test_2_get_schema_version(self):
        """ test_2 get_schema_version """
        field_definition_passed = {"externalProcess": "schema_api_version"}
        schema_api_version = 5
        returned_schema_api_version = perform_additional_processing({}, field_definition_passed, schema_api_version)
        self.assertTrue(returned_schema_api_version == schema_api_version)

    def test_3_get_file_created_date(self):
        """ test_3 get_file_created_date """
        field_definition_passed = {"externalProcess": "file_created_date"}
        returned_value = perform_additional_processing({}, field_definition_passed, "")
        expected_value = str(date.today())
        self.assertTrue(returned_value == expected_value)

    def test_4_get_repository_name_from_ead_resource(self):
        """ test_4 get_repository_name_from_ead_resource """
        ead_resource = "oai:und//repositories/3/resources/1569"
        returned_value = get_repository_name_from_ead_resource(ead_resource)
        expected_value = "RARE"
        self.assertTrue(returned_value == expected_value)

    def test_5_get_define_manifest_level(self):
        """ test_5 define_manifest_level """
        items = []
        returned_value = define_manifest_level(items)
        expected_value = "manifest"
        self.assertTrue(returned_value == expected_value)
        items = [{"something other than level": True}]
        returned_value = define_manifest_level(items)
        expected_value = "manifest"
        self.assertTrue(returned_value == expected_value)
        items = [{"something other than level": True}, {"level": "manifest"}]
        returned_value = define_manifest_level(items)
        expected_value = "collection"
        self.assertTrue(returned_value == expected_value)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()