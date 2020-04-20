import _set_test_path  # noqa
import unittest
from datetime import datetime, timedelta, timezone
from init.helpers import get_file_ids_to_be_processed, get_all_file_ids

test_config = {
    "process-bucket-csv-basepath": "csv",
    "hours-threshold-for-incremental-harvest": 72
}

now = datetime.utcnow()
now = now.replace(tzinfo=timezone.utc)

s3_query_result = [
    {"Key": "csv/file_in_range.csv", "LastModified": (now - timedelta(hours=test_config['hours-threshold-for-incremental-harvest'] - 5))},
    {"Key": "csv/file_out_of_range.csv", "LastModified": (now - timedelta(hours=test_config['hours-threshold-for-incremental-harvest'] + 5))},
    {"Key": "csv/", "LastModified": (now - timedelta(hours=test_config['hours-threshold-for-incremental-harvest'] - 5))}
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


if __name__ == '__main__':
    unittest.main()
