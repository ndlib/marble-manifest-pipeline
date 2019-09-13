import unittest
import json
from iiifManifest import iiifManifest


class TestProcessCsv(unittest.TestCase):
    def setUp(self):
        with open("../example/item-one-image/config.json", 'r') as input_source:
            config = json.load(input_source)
        input_source.close()

        with open("../example/item-one-image/event.json", 'r') as input_source:
            event_data = json.load(input_source)
        input_source.close()

        self.iiifManifest = iiifManifest(config, event_data)
        pass

    def test_buildJson(self):
        with open('../example/item-one-image/manifest.json') as json_data:
            # The Ordered Dict hook preserves the pair ordering in the file for comparison
            manifest_json = json.load(json_data)
            json_data.close()

        manifest_json = "".join(json.dumps(manifest_json, sort_keys=True).split())
        result_json = "".join(json.dumps(self.iiifManifest.manifest(), sort_keys=True).split())
        self.assertEqual(result_json, manifest_json)


if __name__ == '__main__':
    unittest.main()
