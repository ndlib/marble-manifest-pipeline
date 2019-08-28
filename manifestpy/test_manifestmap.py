import unittest
import os
from manifestmap import mapManifestOfItems


class testManifestMap(unittest.TestCase):
    def test(self):
        path = os.path.dirname(os.path.abspath(__file__))
        opchild = os.path.join(path, 'outputchild0.json')
        testReadfile = {
          'type': 'Manifest',
          'label': 'Test',
          'author': 'Unknown',
          'identifier': 'abc-123',
          'sequences': [{
              'canvases': [{
                  'label': 'cvsNameTest',
                  '@id': 'http://cvstesturl.com',
                  'thumbnail': {
                    'label': 'thumbTest',
                    '@id': 'http://thumbtesturl.com'
                  }
              }]
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
        self.assertEqual(mapManifestOfItems(testReadfile, 'CreativeWork'), testShouldBe)
        self.assertNotEqual(mapManifestOfItems(testReadfile, 'CreativeWorkSeries'), testShouldBe)


if __name__ == '__main__':
    unittest.main()
