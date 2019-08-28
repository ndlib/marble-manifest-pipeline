import unittest
import os
import json
from mapcollection import mapManifestCollection


class testMapMain(unittest.TestCase):
    def test(self):
        path = os.path.dirname(os.path.abspath(__file__))
        opchild = os.path.join(path, 'outputchild0.json')
        with open("test_data.json", 'r') as input_source:
            test_readfile = json.load(input_source)
        input_source.close()
        test_should_be = {'@context': 'http://schema.org', '@type': 'CreativeWork', u'name': 'Test', u'creator': 'Unknown', u'identifier': 'abc-123', u'hasPart': [str(opchild)]}
        self.assertEqual(mapManifestCollection(test_readfile, 'CreativeWork'), test_should_be)
        self.assertNotEqual(mapManifestCollection(test_readfile, 'CreativeWorkSeries'), test_should_be)


if __name__ == '__main__':
    unittest.main()
