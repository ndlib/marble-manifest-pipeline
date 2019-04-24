# test_get_manifest_info.py
""" test get_manifest_info """

import unittest
from get_manifest_info import get_manifest_info


class Test(unittest.TestCase):
    """ Class for test fixtures """

    def test_get_manifest_info_passing_id(self):
        """ compare actual an expected modified xml """
        # Note:  Following should resolve to: https://presentation-iiif.library.nd.edu/ils-000909885/manifest
        manifest_info = get_manifest_info('ils-000909885')
        manifest_found = manifest_info['manifest_found']
        self.assertTrue(manifest_found)
        # verify manifest_found returns false if not found
        manifest_info = get_manifest_info('abc123xyz')
        manifest_found = manifest_info['manifest_found']
        self.assertFalse(manifest_found)

    def test_get_manifest_info_passing_url(self):
        """ compare actual an expected modified xml """
        # Note:  Following should resolve to: https://presentation-iiif.library.nd.edu/ils-000909885/manifest
        manifest_info = get_manifest_info('https://presentation-iiif.library.nd.edu/ils-000909885/manifest')
        manifest_found = manifest_info['manifest_found']
        self.assertTrue(manifest_found)
        # verify manifest_found returns false if not found
        manifest_info = get_manifest_info('abc123xyz')
        manifest_found = manifest_info['manifest_found']
        self.assertFalse(manifest_found)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
