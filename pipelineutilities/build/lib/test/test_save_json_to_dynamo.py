""" test save_json_to_dynamo """
import _set_path  # noqa
import unittest
from datetime import datetime
from pipelineutilities.save_json_to_dynamo import _serialize_json


class Test(unittest.TestCase):
    """ Class for test fixtures """

    def test_01_serialize_json(self):
        """ test_01_serialize_json """
        test_json = {"abc": datetime(2020, 10, 13, 9, 0, 0, 0)}
        expected_results = {'abc': '2020-10-13T09:00:00'}
        actual_results = _serialize_json(test_json)
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    # save_data_for_tests()
    suite()
    unittest.main()
