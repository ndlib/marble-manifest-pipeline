import unittest
import os
from mapcollection import mapManifestCollection1`


class testMapMain(unittest.TestCase):
    def test(self):
        path = os.path.dirname(os.path.abspath(__file__))
        opchild = os.path.join(path, 'outputchild0.json')
        testReadfile = {
          'type': 'Manifest',
          'label': 'Test',
          'author': 'Unknown',
          'identifier': 'abc-123',
          'manifests': [{
          }]
        }
        testShouldBe = {
          '@context': 'http://schema.org',
          '@type': 'CreativeWork',
          u'name': 'Test',
          u'creator': 'Unknown',
          u'identifier': 'abc-123',
          u'hasPart': [str(opchild)]
        }
        self.assertEqual(mapManifestCollection(testReadfile, 'CreativeWork'), testShouldBe)
        self.assertNotEqual(mapManifestCollection(testReadfile, 'CreativeWorkSeries'), testShouldBe)


if __name__ == '__main__':
    unittest.main()
