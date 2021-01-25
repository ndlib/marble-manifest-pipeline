""" test save_json_to_dynamo """
import _set_path  # noqa
import unittest
from datetime import datetime
from pipelineutilities.save_json_to_dynamo import _serialize_json, _build_put_item_parameters


class Test(unittest.TestCase):
    """ Class for test fixtures """

    def test_01_serialize_json(self):
        """ test_01_serialize_json """
        test_json = {"abc": datetime(2020, 10, 13, 9, 0, 0, 0)}
        expected_results = {'abc': '2020-10-13T09:00:00'}
        actual_results = _serialize_json(test_json)
        self.assertEqual(actual_results, expected_results)

    def test_02_build_put_item_parameters(self):
        """ test_02_build_put_item_parameters """
        test_json = {'a': 1, 'b': 2}
        actual_results = _build_put_item_parameters(test_json)
        expected_results = {'Item': {'a': 1, 'b': 2}}
        self.assertEqual(actual_results, expected_results)

    def test_03_build_put_item_parameters(self):
        """ test_03_build_put_item_parameters """
        test_json = {'a': 1, 'b': 2}
        return_values = "ALL_OLD"
        actual_results = _build_put_item_parameters(test_json, return_values)
        expected_results = {'Item': {'a': 1, 'b': 2}, 'ReturnValues': 'ALL_OLD'}
        self.assertEqual(actual_results, expected_results)

    def test_04_build_put_item_parameters(self):
        """ test_04_build_put_item_parameters """
        test_json = {'a': 1, 'b': 2}
        return_values = "ALL_OLD"
        condition_expression = 'attribute_not_exists(PK) AND attribute_not_exists(SK)'
        actual_results = _build_put_item_parameters(test_json, return_values, condition_expression)
        expected_results = {'Item': {'a': 1, 'b': 2}, 'ReturnValues': 'ALL_OLD', 'ConditionExpression': 'attribute_not_exists(PK) AND attribute_not_exists(SK)'}
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    # save_data_for_tests()
    suite()
    unittest.main()
