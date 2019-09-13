import unittest
import json
from processCsv import processCsv


class TestProcessCsv(unittest.TestCase):
    def setUp(self):
        self.ids = [
            'item-one-image',
            'item-multiple-images'
        ]
        pass

    def test_itemOneImageJson(self):
        for id in self.ids:
            print(id)
            data = self.load_data_for_test(id)

            self.csvSet = processCsv(data['config'], data['main_csv'], data['items_csv'], data['image_data'])
            self.csvSet.buildJson()

            event_json = "".join(json.dumps(data['event_json'], sort_keys=True).split())
            result_json = "".join(json.dumps(self.csvSet.result_json, sort_keys=True).split())
            self.assertEqual(result_json, event_json)

    def load_data_for_test(self, id):
        data = {}
        with open("../example/{}/config.json".format(id), 'r') as input_source:
            data['config'] = json.load(input_source)
        input_source.close()

        with open("../example/{}/main.csv".format(id), 'r') as input_source:
            data['main_csv'] = input_source.read()
        input_source.close()

        with open("../example/{}/items.csv".format(id), 'r') as input_source:
            data['items_csv'] = input_source.read()
        input_source.close()

        with open("../example/{}/image-data.json".format(id), 'r') as input_source:
            data['image_data'] = json.load(input_source)
        input_source.close()

        with open('../example/{}/event.json'.format(id), 'r') as json_data:
            # The Ordered Dict hook preserves the pair ordering in the file for comparison
            data['event_json'] = json.load(json_data)
            json_data.close()

        return data


if __name__ == '__main__':
    unittest.main()
