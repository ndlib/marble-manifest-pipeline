# test_get_manifest_info.py
""" test get_manifest_info """

import unittest
from get_manifest_info import append_manifest_info, _get_library, _get_library_collection_code, \
    _get_display_library
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

    def test_get_library(self):
        """ validate results of _get_library """
        manifest_info = {'repository': 'UNDA'}
        self.assertTrue(_get_library(manifest_info) == 'HESB')
        manifest_info = {'repository': 'SPEC'}
        self.assertTrue(_get_library(manifest_info) == 'HESB')
        manifest_info = {'repository': 'snite'}
        self.assertTrue(_get_library(manifest_info) == 'Snite')
        manifest_info = {'repository': 'abc123'}
        self.assertTrue(_get_library(manifest_info) == 'Snite')

    def test_get_library_collection_code(self):
        """ validate results of _get_library_collection_code """
        manifest_info = {'repository': 'UNDA'}
        self.assertTrue(_get_library_collection_code(manifest_info) == 'UNDA ARCHV')
        manifest_info = {'repository': 'SPEC'}
        self.assertTrue(_get_library_collection_code(manifest_info) == 'SPEC')
        manifest_info = {'repository': 'snite'}
        self.assertTrue(_get_library_collection_code(manifest_info) == 'SNITE')
        manifest_info = {'repository': 'abc123'}
        self.assertTrue(_get_library_collection_code(manifest_info) == 'SNITE')

    def test_get_display_library(self):
        """ validate results of _get_display_library """
        manifest_info = {'repository': 'UNDA'}
        self.assertTrue(_get_display_library(manifest_info) == 'University Archives')
        manifest_info = {'repository': 'SPEC'}
        self.assertTrue(_get_display_library(manifest_info) == 'Rare Books and Special Collections')
        manifest_info = {'repository': 'snite'}
        self.assertTrue(_get_display_library(manifest_info) == 'Snite Museum of Art')
        manifest_info = {'repository': 'abc123'}
        self.assertTrue(_get_display_library(manifest_info) == 'Snite')


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
