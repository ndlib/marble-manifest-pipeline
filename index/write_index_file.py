# write_index_file.py
""" Write the index file to disk """

import os
import tarfile
from file_system_utilities import create_directory
# from xml.etree.ElementTree import ElementTree


def write_index_file(index_directory, filename, xml_tree):
    """ write index file to disk for later ingestion into index engine """
    create_directory(index_directory)
    xml_tree.write(index_directory + '/' + filename)
    # note:  xml_delcaration and encoding is not required.
    # Use following command to add the declaration to the top of the XML file
    #    xml_tree.write(
    #        index_directory + '/' + filename,
    #        xml_declaration=True,
    #        encoding="utf-8",
    #        method="xml")
    _create_tar_file(index_directory, filename)
    # os.remove(index_directory + '/' + filename)  # removed to run tests 5/21/19 sm
    return


def _create_tar_file(index_directory, filename):
    """ create tar file for index record """
    with tarfile.open(index_directory + '/' + filename + '.tar.gz', "w:gz") as tar:
        tar.add(index_directory + '/' + filename)
    return
