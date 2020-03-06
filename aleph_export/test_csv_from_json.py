# test_csv_from_json.py
""" test csv_from_json """

import unittest
import json
from pathlib import Path
from csv_from_json import CsvFromJson
from dependencies.pipelineutilities.pipeline_config import get_pipeline_config  # noqa: E402
from dependencies.pipelineutilities.search_files import crawl_available_files


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        self.event = {"local": True}
        self.event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
        self.config = get_pipeline_config(self.event)
        self.csv_field_names = self.config["csv-field-names"]
        self.hash_of_available_files = {}
        if not self.event['local']:
            self.hash_of_available_files = crawl_available_files(self.config)
        else:
            with open('test/sample_hash_of_available_files.json', "r") as input_source:
                self.hash_of_available_files = json.load(input_source)
            input_source.close
        self.csv_from_json_class = CsvFromJson(self.csv_field_names, self.hash_of_available_files)
        with open('test/sample_nd.json', 'r') as input_source:
            self.sample_nd_json = json.load(input_source)
        input_source.close()

    def test_1_return_csv_from_json(self):
        """ Return csv from json. """
        csv_string = self.csv_from_json_class.return_csv_from_json(self.sample_nd_json)
        # with open('test/sample.csv', 'w') as output_file:
        #     output_file.write(csv_string)
        # output_file.close()
        with open('test/sample.csv', 'r') as input_source:
            expected_csv_string = input_source.read()
        input_source.close()
        csv_string = csv_string.replace(chr(13), "")
        expected_csv_string = csv_string.replace(chr(13), "")
        self.assertTrue(csv_string.strip() == expected_csv_string.strip())

    def test_2_file_name_from_filePath(self):
        """ test_2_file_name_from_filePath """
        result = self.csv_from_json_class._file_name_from_filePath("a/b/c/d/e")
        self.assertTrue(result == "e")


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
