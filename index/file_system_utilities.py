# file_system_utilities.py
""" this module contains simple, often-used file system utilities """

import os
import errno


def create_directory(directory):
    """ create directory if it does not exist """
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError as e:  # this allows for running multiple concurrent instances
        if e.errno != errno.EEXIST:
            raise
