# test_get_manifest_info.py
""" test get_manifest_info """

import unittest
from get_manifest_info import append_manifest_info
import json


class Test(unittest.TestCase):
    """ Class for test fixtures """

    def test_get_manifest_info_passing_id(self):
        """ compare actual an expected modified xml """
        # Note:  Following should resolve to: https://presentation-iiif.library.nd.edu/ils-000909885/manifest
        manifest_id = 'ils-000909885'
        with open('./test/manifest_ils-000909885.json', 'r') as input_source:
            manifest_json = json.load(input_source)
        input_source.close()
        manifest_info = append_manifest_info(manifest_id, manifest_json)
        # manifest_info = append_manifest_info('ils-000909885')
        manifest_found = manifest_info['manifest_found']
        self.assertTrue(manifest_found)
        # verify manifest_found returns false if not found
        manifest_info = append_manifest_info('abc123xyz', {})
        manifest_found = manifest_info['manifest_found']
        self.assertFalse(manifest_found)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
