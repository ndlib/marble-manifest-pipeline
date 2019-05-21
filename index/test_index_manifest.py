# test_index_manifest.py
""" test index_manifest """

import unittest
from index_manifest import index_manifest
import os
import json


class Test(unittest.TestCase):
    """ Class for test fixtures """

    def test_index_manifest(self):
        """ test creation of index file for manifest """
        with open("../example/example-input.json", 'r') as input_source:
            data = json.load(input_source)
        input_source.close()
        config = data['config']
        data = {
          "id": "example",
          "config": data["config"],
          "manifestData": data
        }
        doc_id = data['id']
        index_directory = config['local-dir']
        print(index_directory)
        try:
            os.remove(index_directory + '/' + doc_id + '.xml')
            os.remove(index_directory + '/' + doc_id + '.xml.tar.gz')
        except FileNotFoundError:
            pass
        manifest_info = index_manifest(doc_id, config)
        # print(manifest_info)
        self.assertTrue(isinstance(manifest_info, dict))
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
