# extract_field_from_characterization_xml.py
from xml.etree import ElementTree


def extract_field_from_characterization_xml(characterization_string: str, field_to_extract: str) -> str:
    """ Extract a field from the XML characterization given the full path of the field_to_extract """
    characterization_string = _strip_namespaces(characterization_string)
    results = ""
    if isinstance(characterization_string, list):
        characterization_string = characterization_string[0]  # convert to a string
    if characterization_string:
        try:
            xml = ElementTree.fromstring(characterization_string)
            xml = ElementTree.ElementTree(xml)  # make the type an ElementTree instead of an Element
            results = xml.find(field_to_extract).text
        except xml.etree.ElementTree.ParseError:
            pass  # xml was not well formed.
    return results


def _strip_namespaces(xml_string: str) -> str:
    """ In order to simplify xml harvest, we must strip these namespaces """
    namespaces_to_strip = ["ns2:", "ns1:", "ns0:",
                           "xmlns=\"http://hul.harvard.edu/ois/xml/ns/fits/fits_output\"",
                           "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://hul.harvard.edu/ois/xml/ns/fits/fits_output http://hul.harvard.edu/ois/xml/xsd/fits/fits_output.xsd\" version=\"0.6.2\" timestamp=\"12/11/18 8:50 AM\"",  # noqa: E501
                           "\n"
                          ]
    for string in namespaces_to_strip:
        xml_string = xml_string.replace(string, "")
    return xml_string
