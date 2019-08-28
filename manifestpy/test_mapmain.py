import unittest
from mapmain import mapMainManifest


class testMapMain(unittest.TestCase):
    def test(self):
        testReadfile = {
          'type': 'Manifest',
          'label': 'Test',
          'author': 'Unknown',
          'identifier': 'abc-123'
        }
        testShouldBe = {
          '@context': 'http://schema.org',
          '@type': 'CreativeWork',
          u'name': 'Test',
          u'creator': 'Unknown',
          u'identifier': 'abc-123'
        }
        self.assertEqual(mapMainManifest(testReadfile, 'CreativeWork'), testShouldBe)
        self.assertNotEqual(mapMainManifest(testReadfile, 'CreativeWorkSeries'), testShouldBe)


if __name__ == '__main__':
    unittest.main()
