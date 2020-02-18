from iiifManifest import iiifManifest
from MetadataMappings import MetadataMappings


class iiifCollection():
    def __init__(self, config, data):
        self.config = config
        self.data = data

    def manifest(self):
        manifest = iiifManifest(self.config, self.data, self.mappings()).manifest()
        manifest["@context"] = [
            "http://www.w3.org/ns/anno.jsonld",
            "https://presentation-iiif.library.nd.edu/extensions/partiallyDigitized",
            "http://iiif.io/api/presentation/3/context.json"
        ]

        return manifest

    def mappings(self):
        return MetadataMappings(self.data)
