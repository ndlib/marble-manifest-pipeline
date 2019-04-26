# test_modify_existing_index_record.py
""" test modify_existing_index_record """

import unittest
import json
from xml.etree.ElementTree import ElementTree, tostring
from modify_existing_index_record import modify_existing_index_record
from write_index_file import write_index_file


class Test(unittest.TestCase):
    """ Class for test fixtures """

    def test_modify_existing_index_record(self):
        """ compare actual an expected modified xml """
        json_input = json.loads('{"manifest_found": false, "id": "abc123xyz", "repository": "SNITE",'
                                + ' "library": "Snite", "display_library": "Snite Museum of Art"}')
        json_input = json.loads('{"manifest_found": false, "id": "ndu_aleph000909885", "repository": "snite",'
                                + ' "library": "Snite", "display_library": "Snite Museum of Art"}')
        sample_pnx = ElementTree(file='test/ndu_aleph000909885.xml').getroot()
        resulting_pnx = modify_existing_index_record(sample_pnx, json_input)
        self.assertTrue(isinstance(resulting_pnx, ElementTree))
        # need to save resulting_pnx and compare it with known good new pnx
        write_index_file('test/actual_results', 'modified_ndu_aleph000909885.xml', resulting_pnx)
        # now we will read actual and expected results and compare them
        actual_results_file_name = 'test/actual_results/modified_ndu_aleph000909885.xml'
        expected_results_file_name = 'test/expected_results/modified_ndu_aleph000909885.xml'
        actual_results = ElementTree(file=actual_results_file_name)
        expected_results = ElementTree(file=expected_results_file_name)
        # print(ElementTree.tostring(xml_tree.getroot()))
        self.assertTrue(tostring(actual_results.getroot()) == tostring(expected_results.getroot()))


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
