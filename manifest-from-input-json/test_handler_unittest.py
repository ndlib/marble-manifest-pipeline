import unittest
import json
from unittest.mock import patch
from handler import *

with open("../example/example-input.json", 'r') as input_source:
    example_data = json.load(input_source)
input_source.close()


@patch('handler.readS3Json', return_value=example_data)
@patch('handler.writeS3Json')
class TestHandler(unittest.TestCase):
    def setUp(self):
        self.parent_id = 'parent_id'

        with open("../example/example-input.json", 'r') as input_source:
            self.example_data = json.load(input_source)
        input_source.close()

        self.config = self.example_data.get('config')
        pass

    def test_readS3Json_not_called_when_manifest_data_is_passed(self, writeS3Json, readS3Json):
        event = {
            "id": 'parent_id',
            "config": self.example_data.get('config'),
            "manifestData": self.example_data
        }

        run(event, {})
        readS3Json.assert_not_called()

    def test_readS3Json_called_when_no_manifest_data_is_passed(self, writeS3Json, readS3Json):
        event = {
            "id": 'parent_id',
            "config": self.example_data.get('config'),
        }
        run(event, {})
        assert readS3Json.called
        assert writeS3Json.called


if __name__ == '__main__':
    unittest.main()
