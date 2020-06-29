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

    def test_1_add_missing_required_fields(self):
        """ test add_missing_required_fields """
        object = {"id": "test"}
        fixed_object = self.clean_up_content_class._add_missing_required_fields(object)
        expected_object = {"id": "test", "collectionId": "test", "parentId": "root", "apiVersion": self.api_version, "fileCreatedDate": str(date.today())}
        self.assertTrue(fixed_object == expected_object)

    def test_2_define_worktype(self):
        """ test _define_worktype """
        object = {"id": "test", "workType": "something"}
        fixed_object = self.clean_up_content_class._define_worktype(object)
        self.assertTrue(fixed_object["workType"] == "something")
        object["classification"] = "something_else"
        fixed_object = self.clean_up_content_class._define_worktype(object)
        self.assertTrue(fixed_object["workType"] == "something")
        self.assertFalse("classification" in fixed_object)
        object["classification"] = "Decorative Arts, Craft, and Design"
        fixed_object = self.clean_up_content_class._define_worktype(object)
        self.assertTrue(fixed_object["workType"] == "Decorative Arts, Craft, and Design")
        self.assertFalse("classification" in fixed_object)

    def test_3_remove_bad_subjects(self):
        """ test _remove_bad_subjects """
        object = {"id": "test", "subjects": [{"authority": "AAT", "term": "test"}]}
        fixed_object = self.clean_up_content_class._remove_bad_subjects(object)
        self.assertTrue(len(fixed_object["subjects"]) == 1)
        object = {"id": "test", "subjects": [{"authority": "none", "term": "test"}]}
        fixed_object = self.clean_up_content_class._remove_bad_subjects(object)
        self.assertTrue(len(fixed_object["subjects"]) == 0)
        object = {"id": "test", "subjects": [{"term": "test"}]}
        fixed_object = self.clean_up_content_class._remove_bad_subjects(object)
        self.assertTrue(len(fixed_object["subjects"]) == 1)

    def test_4_fix_creators(self):
        """ test _fix_creators """
        object = {"id": "test", "level": "manifest", "creators": [{"fullName": "test name"}]}
        fixed_object = self.clean_up_content_class._fix_creators(object)
        self.assertTrue(fixed_object["creators"][0]["display"] == "test name")
        object = {"id": "test", "creators": [{"fullName": "test name", "display": "other name"}]}
        fixed_object = self.clean_up_content_class._fix_creators(object)
        self.assertTrue(fixed_object["creators"][0]["display"] == "other name")

    def test_5_fix_modified_date(self):
        """ test _fix_modified_date """
        object = {"id": "test", "modifiedDate": "4/7/2020 15:24:47"}
        fixed_object = self.clean_up_content_class._fix_modified_date(object)
        self.assertTrue(fixed_object["modifiedDate"] == "2020-04-07T15:24:47Z")
        object = {"id": "test", "modifiedDate": "not a date, like circa sometime"}
        fixed_object = self.clean_up_content_class._fix_modified_date(object)
        self.assertTrue(fixed_object["modifiedDate"] == "not a date, like circa sometime")

    def test_6_replace_special_characters(self):
        """ test _replace_special_characters """
        starting_string = "something&#39;s wrong with%20special characters"
        fixed_string = self.clean_up_content_class._replace_special_characters(starting_string)
        expected_string = "something's wrong with\nspecial characters"
        self.assertTrue(fixed_string == expected_string)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
