from iiifImage import iiifImage
from iiifCanvas import iiifCanvas
from iiifItem import iiifItem


class iiifManifest(iiifItem):
    def __init__(self, config, manifest_data):
        self.id = config['id']
        iiifItem.__init__(self, self.id, 'Manifest')
        config['event_id'] = config['id']
        self.config = config
        self.manifest_data = manifest_data
        self.validate_manifest_data()

    def validate_manifest_data(self):
        if ('language' not in self.manifest_data):
            self.manifest_data['language'] = 'en'
        if ('viewingDirection' not in self.manifest_data):
            self.manifest_data['viewingDirection'] = 'left-to-right'

    def manifest(self):
        manifest = {
            "@context": [
                "http://www.w3.org/ns/anno.jsonld",
                "http://iiif.io/api/presentation/3/context.json"
            ],
            'type': self.type,
            'id': self._manifest_id(),
            'label': self._add_language(self.manifest_data['label']),
            'metadata': self._convert_metadata(self.manifest_data['metadata']),
            'rights': self.manifest_data['rights'],
            'requiredStatement': self._convert_label_value(self.manifest_data['requiredStatement']),
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
                ret.append(iiifCanvas(item_data, self.config).canvas())
        return ret

    def thumbnail(self):
        default_page = self.manifest_data['items'][0]
        for item in self.manifest_data['items']:
            if item['file'] == self.manifest_data['thumbnail']:
                default_page = item.copy()
                default_page['file'] = 'default'
        return [iiifImage(default_page['file'], self.config).thumbnail()]

    def _manifest_id(self):
        return self.config['manifest-server-base-url'] + '/' + self.id + '/manifest'

    def _convert_metadata(self, metadata):
        ret = []
        for md in metadata:
            ret.append(self._convert_label_value(md))
        return ret

    def _convert_label_value(self, dict):
        if ('label' in dict and 'value' in dict):
            dict['label'] = self._add_language(dict['label'])
            dict['value'] = self._add_language(dict['value'])
            return dict
        return None

    def _add_language(self, text):
        return {self.manifest_data['language']: text}
