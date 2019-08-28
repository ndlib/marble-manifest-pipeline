import unittest
import json
from mapmain import mapMainManifest


class testMapMain(unittest.TestCase):
    def test(self):
        with open("test_data.json", 'r') as input_source:
            test_readfile = json.load(input_source)
        input_source.close()
        test_should_be = {'@context': 'http://schema.org', '@type': 'CreativeWork', u'name': 'Test', u'creator': 'Unknown', u'identifier': 'abc-123'}
        self.assertEqual(mapMainManifest(test_readfile, 'CreativeWork'), test_should_be)
        self.assertNotEqual(mapMainManifest(test_readfile, 'CreativeWorkSeries'), test_should_be)


if __name__ == '__main__':
    unittest.main()
