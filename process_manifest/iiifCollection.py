from iiifManifest import iiifManifest
from iiifItem import iiifItem
from MetadataMappings import MetadataMappings


class iiifCollection(iiifItem):
    def __init__(self, config, data):
        self.id = config['id']
        iiifItem.__init__(self, self.id, 'Collection')
        config['event_id'] = config['id']
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
        return MetadataMappings(self.data.get('sourceSystem', 'rbsc'))
