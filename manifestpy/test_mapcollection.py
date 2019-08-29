import unittest
import os
import json
from mapcollection import mapManifestCollection


class testMapMain(unittest.TestCase):
    def test(self):
        self.parent_id = 'parent_id'
        path = os.path.dirname(os.path.abspath(__file__))
        opchild = os.path.join(path, 'outputchild0.json')
        with open("../example/example-input.json", 'r') as input_source:
            self.example_data = json.load(input_source)
        input_source.close()
        test_should_be = {'@context': 'http://schema.org', '@type': 'CreativeWork', u'hasPart': [str(opchild)], u'url': 'bitter'}
        self.assertEqual(mapManifestCollection(self.example_data, 'CreativeWork'), test_should_be)
        self.assertNotEqual(mapManifestCollection(self.example_data, 'CreativeWorkSeries'), test_should_be)


if __name__ == '__main__':
    unittest.main()
