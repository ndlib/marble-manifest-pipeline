# test_get_existing_index_record.py
""" test get_existing_index_record """

import unittest
from xml.etree.ElementTree import Element
from get_existing_index_record import get_existing_index_record


class Test(unittest.TestCase):
    """ Class for test fixtures """

    def test_get_existing_index_record(self):
        """ Call get_existing_index_record passing a known docid to verify Element is returned. """
        docid = 'ndu_aleph000909884'
        resulting_pnx = get_existing_index_record(docid)
        self.assertTrue(isinstance(resulting_pnx, Element))


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
