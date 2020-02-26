from iiifManifest import iiifManifest
from MetadataMappings import MetadataMappings


class iiifCollection():
    def __init__(self, config, data):
        self.config = config
        self.data = data
        self.mappings = MetadataMappings(self.data)
        self.document = iiifManifest(self.config, self.data, self.mappings)

    def manifest(self):
        manifest = self.document.manifest()
        

        return manifest
