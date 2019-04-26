# get_existing_index_record.py  4/4/19
""" Retrieves xml of existing pnx index record """

# import sys
import re
from urllib import request, error
from xml.etree.ElementTree import fromstring


def get_existing_index_record(recordid):
    """ Try to find an existing index record and return it """
    try:
        index_record = _get_pnx_xml_given_recordid(recordid, True)
    except error.HTTPError:
        print('Index record not found in sandbox for recordid ' + recordid)
        try:
            index_record = _get_pnx_xml_given_recordid(recordid, False)
        except error.HTTPError:
            print('Index record not found in production for recordid ' + recordid)
            raise
    return index_record


def _get_pnx_xml_given_recordid(recordid, use_sandbox=True):
    """ Retrieves pnx xml given an existing unique recordid."""
    # sample docid: ndu_aleph000909884 or nduspec_ead7s75db80w4r
    # change url to here: https://a1fc3ld3d7.execute-api.us-east-1.amazonaws.com/dev/ then follow with   primo/v1...
    root_pnx_url = 'https://a1fc3ld3d7.execute-api.us-east-1.amazonaws.com/dev/'
    pnx_url = root_pnx_url + 'primo/v1/search/xml/L/' \
        + recordid + '?lang=eng&adaptor=Local%20Search%20Engine&showPnx=true&inst=NDU'
    if use_sandbox:
        pnx_url = pnx_url + '&sandbox=1'  # force to use production server for search
    # pnx_url = 'https://api-na.hosted.exlibrisgroup.com/primo/v1/search/xml/L/' \
    # + docid + '?lang=eng&adaptor=Local%20Search%20Engine&showPnx=true&inst=NDU&apikey='
    pnx_string = ""
    # print(pnxurl)
    try:
        pnx_string = request.urlopen(pnx_url).read()
        # print('pnx was read')
        pnx_string = pnx_string.decode("utf-8")  # convert from byte object to str
        # print('string was converted')
    except error.HTTPError:
        # print('cannot retrieve xml from ' + pnx_url + ' in get_pnx_xml_given_docid: ' + recordid)
        raise
    pnx_as_xml = _convert_pnx_string_to_xml(pnx_string)
    return pnx_as_xml


def _convert_pnx_string_to_xml(pnx_string):
    pnx_string_without_namespace = _strip_namespace_from_pnx_string(pnx_string)
    pnx_string_without_newlines = _strip_newlines_from_pnx_string(pnx_string_without_namespace)
    pnx_as_xml = _convert_string_to_xml(pnx_string_without_newlines)
    return(pnx_as_xml)


def _strip_namespace_from_pnx_string(pnx_string):
    """ The original pnx record contains namespaces, which cause problems for ElementTree, so we must strip them """
    pnx_string_without_namespace = re.sub(' xmlns="[^"]+"', '', pnx_string)  # , count=2)
    return(pnx_string_without_namespace)


def _strip_newlines_from_pnx_string(pnx_string):
    """ The original pnx record contains "\n", which cause problems for ElementTree, so we must strip them """
    pnx_string_without_newlines = re.sub('\n', '', pnx_string)
    return(pnx_string_without_newlines)


def _convert_string_to_xml(pnx_string):
    """ Convert string to XML so we can manipulate it further. """
    pnx_as_xml = fromstring(pnx_string)
    return(pnx_as_xml)


# if __name__ == "__main__":
#     pnx_filename = ''
#     print(len(sys.argv))
#     if len(sys.argv) >= 1:
#         pnx_filename = ''.join(sys.argv[1])
#     if pnx_filename > '':
#         get_pnx_xml_given_docid(pnx_filename)
