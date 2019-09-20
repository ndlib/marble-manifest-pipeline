import unittest
import json
from create_manifest.iiifCollection import iiifCollection
from test_utils import load_data_for_test


class TestCreateManifest(unittest.TestCase):
    def setUp(self):
        self.ids = [
            'collection-small',
            'item-one-image',
            'item-multiple-images',
            'item-minimal-data'
        ]
        pass

    def test_buildJson(self):
        for id in self.ids:
            print("Testing id, {}".format(id))
            data = load_data_for_test(id)

            self.iiifCollection = iiifCollection(data['config'], data['event_data'], data['image_data'])
            manifest_json = "".join(json.dumps(data['manifest_json'], sort_keys=True).split())
            result_json = "".join(json.dumps(self.iiifCollection.manifest(), sort_keys=True).split())

            self.assertEqual(result_json, manifest_json)


if __name__ == '__main__':
    unittest.main()
