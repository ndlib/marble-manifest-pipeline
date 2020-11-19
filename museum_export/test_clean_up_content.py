# test_clean_up_content.py
""" test clean_up_content """
import _set_path  # noqa
import unittest
from clean_up_content import CleanUpContent
from datetime import date


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        self.api_version = 1
        self.clean_up_content_class = CleanUpContent({}, {}, self.api_version)

    def test_01_add_missing_required_fields(self):
        """ test_01 add_missing_required_fields """
        object = {"id": "test"}
        fixed_object = self.clean_up_content_class._add_missing_required_fields(object)
        expected_object = {"id": "test", "collectionId": "test", "parentId": "root", "apiVersion": self.api_version, "fileCreatedDate": str(date.today())}
        self.assertEqual(fixed_object, expected_object)

    def test_02_define_worktype(self):
        """ test_02 _define_worktype """
        object = {"id": "test", "workType": "something"}
        fixed_object = self.clean_up_content_class._define_worktype(object)
        self.assertEqual(fixed_object["workType"], "something")
        object["classification"] = "something_else"
        fixed_object = self.clean_up_content_class._define_worktype(object)
        self.assertEqual(fixed_object["workType"], "something")
        self.assertFalse("classification" in fixed_object)
        object["classification"] = "Decorative Arts, Craft, and Design"
        fixed_object = self.clean_up_content_class._define_worktype(object)
        self.assertEqual(fixed_object["workType"], "Decorative Arts, Craft, and Design")
        self.assertFalse("classification" in fixed_object)

    def test_03_remove_bad_subjects(self):
        """ test_03 _remove_bad_subjects """
        object = {"id": "test", "subjects": [{"authority": "AAT", "term": "test"}]}
        fixed_object = self.clean_up_content_class._remove_bad_subjects(object)
        self.assertEqual(len(fixed_object["subjects"]), 1)
        object = {"id": "test", "subjects": [{"authority": "none", "term": "test"}]}
        fixed_object = self.clean_up_content_class._remove_bad_subjects(object)
        self.assertEqual(len(fixed_object["subjects"]), 0)
        object = {"id": "test", "subjects": [{"term": "test"}]}
        fixed_object = self.clean_up_content_class._remove_bad_subjects(object)
        self.assertEqual(len(fixed_object["subjects"]), 1)

    def test_04_fix_creators(self):
        """ test_04 _fix_creators """
        object = {"id": "test", "level": "manifest", "creators": [{"fullName": "test name"}]}
        fixed_object = self.clean_up_content_class._fix_creators(object)
        self.assertEqual(fixed_object["creators"][0]["display"], "test name")
        object = {"id": "test", "creators": [{"fullName": "test name", "display": "other name"}]}
        fixed_object = self.clean_up_content_class._fix_creators(object)
        self.assertEqual(fixed_object["creators"][0]["display"], "other name")

    def test_05_fix_modified_date(self):
        """ test_05 _fix_modified_date """
        object = {"id": "test", "modifiedDate": "4/7/2020 15:24:47"}
        fixed_object = self.clean_up_content_class._fix_modified_date(object)
        self.assertEqual(fixed_object["modifiedDate"], "2020-04-07T15:24:47Z")
        object = {"id": "test", "modifiedDate": "not a date, like circa sometime"}
        fixed_object = self.clean_up_content_class._fix_modified_date(object)
        self.assertEqual(fixed_object["modifiedDate"], "not a date, like circa sometime")

    def test_06_replace_special_characters(self):
        """ test_06 _replace_special_characters """
        starting_string = "something&#39;s wrong with%20special characters"
        fixed_string = self.clean_up_content_class._replace_special_characters(starting_string)
        expected_string = "something's wrong with\nspecial characters"
        self.assertEqual(fixed_string, expected_string)

    def test_07_get_parent_child_id_list(self):
        """ test_07_get_parent_child_id_list """
        standard_json = {"id": "1", "items": [{"id": "1a"}, {"id": "1b", "items": [{"id": "1b.1"}, {"id": "1b.2"}]}]}
        actual_results = self.clean_up_content_class._get_parent_child_id_list(standard_json)
        expected_results = ['1', '1a', '1b', '1b.1', '1b.2']
        self.assertEqual(actual_results, expected_results)

    def test_08_remove_unnecessary_relatedIds(self):
        """ test_08_remove_unnecessary_relatedIds """
        parent_child_id_list = ['1', '1a', '1b', '1b.1', '1b.2']
        standard_json = {"id": "1", "items": [{"id": "1a", "relatedIds": [{"id": "1b"}, {"id": "something_else"}]}, {"id": "1b", "relatedIds": [{"id": "1a"}], "items": [{"id": "1b.1", "relatedIds": [{"id": "1b.2"}]}, {"id": "1b.2"}]}]}  # noqa: #501
        actual_results = self.clean_up_content_class._remove_unnecessary_relatedIds(standard_json, parent_child_id_list)
        expected_results = {"id": "1", "items": [{"id": "1a", "relatedIds": [{"id": "something_else"}]}, {"id": "1b", "items": [{"id": "1b.1"}, {"id": "1b.2"}]}]}
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
