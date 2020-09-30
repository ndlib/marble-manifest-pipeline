import _set_path  # noqa
import unittest
from pathlib import Path
from pipeline_config import setup_pipeline_config
import datetime
from api_helpers import success, error

base_config = {}
base_config['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
base_config['local'] = True
base_config['test'] = True
config = setup_pipeline_config(base_config)


class Test(unittest.TestCase):

    def test_success_method(self):
        test_response = {
            "statusCode": 200,
            "body": '"hi"',
            "headers": {
                "Access-Control-Allow-Origin": "*",
            },
        }
        self.assertEqual(success("hi"), test_response)

    def test_success_method_converts_datatime_objects(self):
        now = datetime.datetime.now()
        test_response = {
            "statusCode": 200,
            "body": '{"datetime": "' + now.isoformat() + '", "array": ["1", "2", "3"]}',
            "headers": {
                "Access-Control-Allow-Origin": "*",
            },
        }
        test_data = {
            "datetime": now,
            "array": [
                "1", "2", "3"
            ]
        }
        self.assertEqual(success(test_data), test_response)

    def test_error_response(self):
        test_response = {
            "statusCode": 404,
            "headers": {
                "Access-Control-Allow-Origin": "*",
            },
        }
        self.assertEqual(error(404), test_response)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    # save_data_for_tests()
    suite()
    unittest.main()
