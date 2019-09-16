import unittest
import json
from iiifManifest import iiifManifest


class TestProcessCsv(unittest.TestCase):
    def setUp(self):
        self.ids = [
            'item-one-image',
            'item-multiple-images',
            'item-minimal-data'
        ]
        pass

    def test_buildJson(self):
        for id in self.ids:
            print("Testing id, {}".format(id))
            data = self.load_data_for_test(id)

            self.iiifManifest = iiifManifest(data['config'], data['event_data'])
            manifest_json = "".join(json.dumps(data['manifest_json'], sort_keys=True).split())
            result_json = "".join(json.dumps(self.iiifManifest.manifest(), sort_keys=True).split())

            self.assertEqual(result_json, manifest_json)

    def load_data_for_test(self, id):
        data = {}

        with open("../example/{}/config.json".format(id), 'r') as input_source:
            data['config'] = json.load(input_source)
        input_source.close()

        with open("../example/{}/event.json".format(id), 'r') as input_source:
            data['event_data'] = json.load(input_source)
        input_source.close()

        with open('../example/{}/manifest.json'.format(id), 'r') as input_source:
            data['manifest_json'] = json.load(input_source)
        input_source.close()

        return data

if __name__ == '__main__':
    unittest.main()
