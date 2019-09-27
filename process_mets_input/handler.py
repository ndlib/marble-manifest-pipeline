import xml.etree.ElementTree as ET


def run(event, context):
    tree = ET.parse('../example/item-one-image/structural_metadata_mets.xml')
    root = tree.getroot()

    return root.findall('mets:file')


# python -c 'from handler import *; test()'
def test():
    print(run({}, {}))
