""" test save_json_to_dynamo """
import _set_path  # noqa
import unittest
from unittest.mock import patch
from datetime import datetime, timezone
from pipelineutilities.save_json_to_dynamo import SaveJsonToDynamo, _serialize_json, _get_expire_time


class Test(unittest.TestCase):
    """ Class for test fixtures """

    def test_01_serialize_json(self):
        """ test_01_serialize_json """
        test_json = {"abc": datetime(2020, 10, 13, 9, 0, 0, 0)}
        expected_results = {'abc': '2020-10-13T09:00:00'}
        actual_results = _serialize_json(test_json)
        self.assertEqual(actual_results, expected_results)

    def test_02_get_expire_time(self):
        """ test_02_get_expire_time """
        actual_results = _get_expire_time(datetime(2020, 10, 13, 9, 0, 0, 0, tzinfo=timezone.utc), 3)
        expected_results = 1602838800
        self.assertEqual(actual_results, expected_results)

    @patch('pipelineutilities.save_json_to_dynamo._get_expire_time')
    def test_03_init_save_json_to_dynamo(self, mock_get_expire_time):
        """ test_03_init_save_json_to_dynamo """
        mock_get_expire_time.return_value = 1602853200
        config = {"local": True}
        save_json_to_dynamo_class = SaveJsonToDynamo(config, 'some-table-name')
        self.assertEqual(save_json_to_dynamo_class.expire_time, 1602853200)
        self.assertEqual(save_json_to_dynamo_class.dynamo_table_available, False)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    # save_data_for_tests()
    suite()
    unittest.main()
