# test_index_manifest.py
""" test index_manifest """

import unittest
from index_manifest import index_manifest
import os


class Test(unittest.TestCase):
    """ Class for test fixtures """

    def test_index_manifest(self):
        """ test creation of index file for manifest """
        # Note:  Following should resolve to: https://presentation-iiif.library.nd.edu/ils-000909885/manifest
        doc_id = 'ils-000909885'
        index_directory = '/tmp/index'
        try:
            os.remove(index_directory + '/' + doc_id + '.xml')
            os.remove(index_directory + '/' + doc_id + '.xml.tar.gz')
        except FileNotFoundError:
            pass
        manifest_info = index_manifest(doc_id)
        # print(manifest_info)
        self.assertTrue(isinstance(manifest_info, dict))
        # Now that we are writing to an S3 bucket and removing locally, I removed the following lines
        # self.assertTrue(os.path.exists(index_directory + '/' + doc_id + '.xml'))
        # self.assertTrue(os.path.exists(index_directory + '/' + doc_id + '.xml.tar.gz'))
        # clean up after ourseleves
        try:
            os.remove(index_directory + '/' + doc_id + '.xml')
            os.remove(index_directory + '/' + doc_id + '.xml.tar.gz')
        except FileNotFoundError:
            pass


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
