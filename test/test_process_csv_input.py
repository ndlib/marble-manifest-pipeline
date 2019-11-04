import unittest
import json
from process_csv_input.CsvToSchema import CsvToSchema
from test.test_utils import load_data_for_test
# from test.test_utils import debug_json


class TestProcessCsvOutput(unittest.TestCase):
    def setUp(self):
        self.ids = [
            'item-one-image',
            # 'item-multiple-images',
            # 'item-minimal-data'
        ]
        pass

    def test_itemOneImageJson(self):
        for id in self.ids:
            print("Testing id, {}".format(id))
            data = load_data_for_test(id)

            self.csvSet = CsvToSchema(data['config'], data['main_csv'], data['items_csv'], data['image_data'])
            # debug_json(data['schema_json'], self.csvSet.get_json())

            schema_json = "".join(json.dumps(data['schema_json'], sort_keys=True).split())
            result_json = "".join(json.dumps(self.csvSet.get_json(), sort_keys=True).split())

            self.assertEqual(result_json, schema_json)


if __name__ == '__main__':
    unittest.main()
