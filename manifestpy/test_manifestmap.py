import unittest
import os
import json
from manifestmap import mapManifestOfItems


class testManifestMap(unittest.TestCase):
    def test(self):
        path = os.path.dirname(os.path.abspath(__file__))
        opchild = os.path.join(path, 'outputchild0.json')
        with open("../example/example-input.json", 'r') as input_source:
            test_readfile = json.load(input_source)
        input_source.close()

        test_should_be = {'@context': 'http://schema.org', '@type': 'CreativeWork', 'hasPart': [str(opchild)],  u'url': u'bitter'}
        self.assertEqual(mapManifestOfItems(test_readfile, 'CreativeWork'), test_should_be)
        self.assertNotEqual(mapManifestOfItems(test_readfile, 'CreativeWorkSeries'), test_should_be)


if __name__ == '__main__':
    unittest.main()
