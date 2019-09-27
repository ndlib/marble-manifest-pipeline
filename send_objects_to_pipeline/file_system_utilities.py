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


def delete_file(folder_name, file_name):
    """ Delete temparary intermediate file """
    full_path_file_name = get_full_path_file_name(folder_name, file_name)
    try:
        os.remove(full_path_file_name)
    except FileNotFoundError:
        pass


def get_full_path_file_name(folder_name, file_name):
    """ Build full path to file given folder and file name """
    full_path_file_name = ''
    if folder_name > '':
        full_path_file_name = folder_name
        if full_path_file_name[len(full_path_file_name) - 1:] != '/':
            full_path_file_name += '/'
    full_path_file_name += file_name
    return full_path_file_name
