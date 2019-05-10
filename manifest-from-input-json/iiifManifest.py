from iiifImage import iiifImage
from iiifCanvas import iiifCanvas
from iiifItem import iiifItem


class iiifManifest(iiifItem):
    def __init__(self, id, event_config, manifest_data):
        iiifItem.__init__(self, id, 'Manifest')
        event_config['event_id'] = id
        self.config = event_config
        self.manifest_data = manifest_data

    def manifest(self):
        manifest = {
            "@context": [
                "http://www.w3.org/ns/anno.jsonld",
                "http://iiif.io/api/presentation/3/context.json"
            ],
            'type': self.type,
            'id': self._manifest_id(),
            'label': self.manifest_data['label'],
            'metadata': self.manifest_data['metadata'],
            'rights': self.manifest_data['rights'],
            'requiredStatement': self.manifest_data['requiredStatement'],
            'viewingDirection': self.manifest_data['viewingDirection'],
            'thumbnail': self.thumbnail(),
            'items': self._items()
        }
        # add optional data
        if 'homepage' in self.manifest_data:
            manifest['homepage'] = self.manifest_data['homepage']
        if 'seeAlso' in self.manifest_data:
            manifest['seeAlso'] = self.manifest_data['seeAlso']
        return manifest

    def _items(self):
        ret = []
        if 'items' in self.manifest_data:
            for item_data in self.manifest_data['items']:
                ret.append(iiifCanvas(item_data['file'], item_data['label'], self.config).canvas())
        return ret

    def thumbnail(self):
        default_page = self.manifest_data['items'][0]
        for item in self.manifest_data['items']:
            if item['file'] == self.config['default-img']:
                default_page = item.copy()
                default_page['file'] = 'default'
        return [iiifImage(default_page['file'], self.config).thumbnail()]

    def _manifest_id(self):
        return self.config['manifest-server-base-url'] + '/' + self.id + '/manifest'
