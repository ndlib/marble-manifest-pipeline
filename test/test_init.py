import _set_test_path  # noqa
import unittest
import re
import json
from datetime import datetime, timedelta, timezone
from init.helpers import get_file_ids_to_be_processed, get_all_file_ids
from init.handler import run

test_config = {
    "process-bucket-data-basepath": "json",
    "hours-threshold-for-incremental-harvest": 72
}

now = datetime.utcnow()
now = now.replace(tzinfo=timezone.utc)

s3_query_result = [
    {"Key": "json/file_in_range.json", "LastModified": (now - timedelta(hours=test_config['hours-threshold-for-incremental-harvest'] - 5))},
    {"Key": "json/file_out_of_range.json", "LastModified": (now - timedelta(hours=test_config['hours-threshold-for-incremental-harvest'] + 5))},
    {"Key": "json/", "LastModified": (now - timedelta(hours=test_config['hours-threshold-for-incremental-harvest'] - 5))}
]


class TestSearchFiles(unittest.TestCase):

    def test_get_file_ids_to_be_processed(self):
        # normal 72 hour window
        result = get_file_ids_to_be_processed(s3_query_result, test_config)
        self.assertEqual(list(result), ['file_in_range'])

        # move the window so it now longer returns the in range file
        test_config["hours-threshold-for-incremental-harvest"] = 24
        result = get_file_ids_to_be_processed(s3_query_result, test_config)
        self.assertEqual(list(result), [])

        # move the window so they both appear
        test_config["hours-threshold-for-incremental-harvest"] = 100
        result = get_file_ids_to_be_processed(s3_query_result, test_config)
        self.assertEqual(list(result), ['file_in_range', 'file_out_of_range'])

    def test_get_all_file_ids(self):
        # should only filter the main directory out
        result = get_all_file_ids(s3_query_result, test_config)
        self.assertEqual(list(result), ['file_in_range', 'file_out_of_range'])

    def test_handler_returns_correct_event(self):
        input = {
            "local": True,
            "ids": ['id!']
        }
        event = run(input, {})

        # it should add config filename with a date pattern 2020-04-21-10:49:17.707361.json
        self.assertEqual(re.fullmatch(r"^([0-9]{4}[-][0-9]{2}-[0-9]{2}[-][0-9]{2}[:][0-9]{2}[:][0-9]{2}[.][0-9]{6}[.]json)$", event['config-file']).group(0), event['config-file'])
        self.assertEqual(event['process-bucket'], 'marble-manifest-prod-processbucket-13bond538rnnb')
        self.assertEqual(event['local'], True)
        self.assertEqual(event['errors'], [])

        ecs_args = event.get('ecs-args')
        del event['ecs-args']
        self.assertEqual(ecs_args, [json.dumps(event)])


if __name__ == '__main__':
    unittest.main()
