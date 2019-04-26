# test_write_index_file.py
""" test write_index_file.py """

import unittest
import os
from write_index_file import write_index_file
from xml.etree.ElementTree import ElementTree, Element


class Test(unittest.TestCase):
    """ Class for test fixtures """

    def test_write_index_file(self):
        """ test write_index_file """
        # if file already exists, delete it
        index_directory = '/tmp/index/'
        filename = 'xyz.xml'
        root = Element("records")
        xml_tree = ElementTree(root)
        try:
            os.remove(index_directory + '/' + filename)
            os.remove(index_directory + '/' + filename + '.tar.gz')
        except OSError:
            pass
        self.assertFalse(os.path.exists(index_directory + '/' + filename))
        # write file
        write_index_file(index_directory, filename, xml_tree)
        self.assertTrue(os.path.exists(index_directory + '/' + filename))
        self.assertTrue(os.path.exists(index_directory + '/' + filename + '.tar.gz'))
        # try to write file again, over existing file, to verify this works too.
        write_index_file(index_directory, filename, xml_tree)
        self.assertTrue(os.path.exists(index_directory + '/' + filename))
        self.assertTrue(os.path.exists(index_directory + '/' + filename + '.tar.gz'))
        # clean up after ourselves
        try:
            os.remove(index_directory + '/' + filename)
            os.remove(index_directory + '/' + filename + '.tar.gz')
        except OSError:
            pass
        self.assertFalse(os.path.exists(index_directory + '/' + filename))


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
