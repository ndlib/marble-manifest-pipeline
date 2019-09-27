import xml.etree.ElementTree as ET


class iiifManifest(iiifItem):

    def __init__(self, sequence_file, metadata_file):
        self.id = config['id']
