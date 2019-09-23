import unittest
import json
from process_csv_input.ProcessCsvInput import ProcessCsvInput
from test.test_utils import load_data_for_test


class TestProcessCsvOutput(unittest.TestCase):
    def setUp(self):
        self.ids = [
            'item-one-image',
            'item-multiple-images',
            'item-minimal-data'
        ]
        pass

    def test_itemOneImageJson(self):
        for id in self.ids:
            print("Testing id, {}".format(id))
            data = load_data_for_test(id)

            self.csvSet = ProcessCsvInput(data['config'], data['main_csv'], data['items_csv'], data['image_data'])
            self.csvSet.buildJson()

            event_json = "".join(json.dumps(data['event_json'], sort_keys=True).split())
            result_json = "".join(json.dumps(self.csvSet.result_json, sort_keys=True).split())

            self.assertEqual(result_json, event_json)


if __name__ == '__main__':
    unittest.main()
