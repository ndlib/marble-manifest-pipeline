# test_file_system_utilities
""" test file_system_utilities.py """

import os
import shutil
import unittest
from file_system_utilities import create_directory


class Test(unittest.TestCase):
    """ Class for test fixtures """

    def test_create_directory(self):
        """ test create_directory """
        directory_name = 'xyz'
        # Make sure directory does not exist
        if os.path.exists(directory_name):
            shutil.rmtree(directory_name, ignore_errors=True)
        self.assertFalse(os.path.exists(directory_name))
        # Try to create directory, then verify it exists
        create_directory(directory_name)
        self.assertTrue(os.path.exists(directory_name))
        # Try to create directory again, even though it exists
        create_directory(directory_name)
        self.assertTrue(os.path.exists(directory_name))
        # Clean up after ourselves
        if os.path.exists(directory_name):
            shutil.rmtree(directory_name, ignore_errors=True)
        self.assertFalse(os.path.exists(directory_name))


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
