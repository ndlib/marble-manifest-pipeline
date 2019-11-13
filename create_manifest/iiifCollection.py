from iiifManifest import iiifManifest
from iiifItem import iiifItem


class iiifCollection(iiifItem):
    def __init__(self, config, schema):
        self.id = config['id']
        iiifItem.__init__(self, self.id, 'Collection')
        config['event_id'] = config['id']
        self.config = config
        self.schema = schema

    def manifest(self):
        manifest = iiifManifest(self.config, self.schema).manifest()
        manifest["@context"] = [
            "http://www.w3.org/ns/anno.jsonld",
            "https://presentation-iiif.library.nd.edu/extensions/partiallyDigitized",
            "http://iiif.io/api/presentation/3/context.json"
        ]

        return manifest
