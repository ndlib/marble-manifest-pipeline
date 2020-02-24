# test_transform_marc_json.py
""" test transform_marc_json """

import unittest
import json
from pathlib import Path
from transform_marc_json import TransformMarcJson
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
        self.transform_marc_json_class = TransformMarcJson(self.csv_field_names, self.hash_of_available_files)
        filename = 'test/sample_marc.json'
        with open(filename, 'r') as input_source:
            self.marc_record_as_json = json.load(input_source)
        input_source.close()

    def test_1_build_json_from_marc_json(self):
        """ Write header and a record, and verify what was written. """
        nd_json = self.transform_marc_json_class.build_json_from_marc_json(self.marc_record_as_json)
        # with open('test/sample_nd.json', 'w') as output_file:
        #     json.dump(nd_json, output_file, indent=2, default=str)
        # output_file.close()
        with open('test/sample_nd.json', 'r') as input_source:
            expected_json = json.load(input_source)
        input_source.close()
        self.assertTrue(nd_json == expected_json)
        csv_string = self.transform_marc_json_class.create_csv_from_json(nd_json)
        # with open('test/sample.csv', 'w') as output_file:
        #     output_file.write(csv_string)
        # output_file.close()
        with open('test/sample.csv', 'r') as input_source:
            expected_csv_string = input_source.read()
        input_source.close()
        csv_string = csv_string.replace(chr(13), "")
        expected_csv_string = expected_csv_string.replace(chr(13), "")
        self.assertTrue(csv_string.strip() == expected_csv_string.strip())

    def test_2_lookup_work_type(self):
        """ test _lookup_work_type"""
        result = self.transform_marc_json_class._lookup_work_type("a")
        self.assertTrue(result == "Language material")


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
