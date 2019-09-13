import unittest
import json
from processCsv import processCsv


class TestProcessCsv(unittest.TestCase):
    def setUp(self):
        with open("../example/item-one-image/config.json", 'r') as input_source:
            config = json.load(input_source)
        input_source.close()

        with open("../example/item-one-image/main.csv", 'r') as input_source:
            main_csv = input_source.read()
        input_source.close()
        with open("../example/item-one-image/items.csv", 'r') as input_source:
            items_csv = input_source.read()
        input_source.close()
        with open("../example/item-one-image/image-data.json", 'r') as input_source:
            image_data = json.load(input_source)
        input_source.close()

        self.csvSet = processCsv(config, main_csv, items_csv, image_data)
        pass

    def test_buildJson(self):
        self.csvSet.buildJson()
        with open('../example/item-one-image/event.json') as json_data:
            # The Ordered Dict hook preserves the pair ordering in the file for comparison
            event_json = json.load(json_data)
            json_data.close()
        event_json = "".join(json.dumps(event_json, sort_keys=True).split())
        result_json = "".join(json.dumps(self.csvSet.result_json, sort_keys=True).split())
        self.assertEqual(result_json, event_json)


if __name__ == '__main__':
    unittest.main()
