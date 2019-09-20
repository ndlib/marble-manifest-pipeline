from iiifManifest import iiifManifest
from iiifItem import iiifItem


class iiifCollection(iiifItem):
    def __init__(self, config, manifest_data, image_data):
        self.id = config['id']
        iiifItem.__init__(self, self.id, 'Collection')
        config['event_id'] = config['id']
        self.config = config
        self.manifest_data = manifest_data
        self.image_data = image_data

    def manifest(self):
        manifest = iiifManifest(self.config, self.manifest_data, self.image_data)
        manifest["@context"] = [
            "http://www.w3.org/ns/anno.jsonld",
            "http://iiif.io/api/presentation/3/context.json"
        ]

        return manifest
