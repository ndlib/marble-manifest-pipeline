import unittest
import json
from process_mets_input.MetsToSchema import MetsToSchema
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
            self.metsSet = MetsToSchema(data['config'], data['descriptive_metadata'], data['structural_metadata'], data['image_data'])
            # debug_json(data['schema_json'], self.metsSet.get_json())

            schema_json = "".join(json.dumps(data['schema_json'], sort_keys=True).split())
            result_json = "".join(json.dumps(self.metsSet.get_json(), sort_keys=True).split())

            self.assertEqual(result_json, schema_json)


if __name__ == '__main__':
    unittest.main()
