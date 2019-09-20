from create_manifest.iiifImage import iiifImage
from create_manifest.iiifCanvas import iiifCanvas
from create_manifest.iiifItem import iiifItem
from pathlib import Path


class iiifManifest(iiifItem):
    def __init__(self, config, manifest_data, image_data):
        self.id = config['id']
        iiifItem.__init__(self, self.id, manifest_data['manifest-type'])
        config['event_id'] = config['id']
        self.config = config
        self.manifest_data = manifest_data
        self.image_data = image_data

    def manifest(self):
        manifest = {
            'type': self.type,
            'id': self._manifest_id(),
            'label': self._lang_wrapper(self.manifest_data['label']),
            'thumbnail': self.thumbnail(),
            'items': self._items()
        }

        if 'metadata' in self.manifest_data:
            manifest['metadata'] = self._convert_metadata(self.manifest_data['metadata'])
        if 'rights' in self.manifest_data:
            manifest['rights'] = self.manifest_data['rights']
        if 'requiredStatement' in self.manifest_data:
            manifest['requiredStatement'] = self._convert_label_value(self.manifest_data['requiredStatement'])
        if 'viewingDirection' in self.manifest_data:
            manifest['viewingDirection'] = self.manifest_data['viewingDirection']
        else:
            manifest['viewingDirection'] = 'left-to-right'

        if 'homepage' in self.manifest_data:
            manifest['homepage'] = self.manifest_data['homepage']
        if 'seeAlso' in self.manifest_data:
            manifest['seeAlso'] = self.manifest_data['seeAlso']
            for index, seeAlso in enumerate(manifest['seeAlso']):
                if (manifest['seeAlso'][index].get('label', False)):
                    manifest['seeAlso'][index]['label'] = self._lang_wrapper(manifest['seeAlso'][index]['label'])

        return manifest

    def _items(self):
        ret = []
        if 'items' in self.manifest_data:
            for item_data in self.manifest_data['items']:
                if (item_data['manifest-type'] == 'image'):
                    file = Path(item_data['file']).stem
                    item_data['width'] = self.image_data[file]['width']
                    item_data['height'] = self.image_data[file]['height']

                    ret.append(iiifCanvas(item_data, self.config).canvas())
                elif (item_data['manifest-type'] == 'manifest'):
                    tempConfig = self.config
                    tempConfig['id'] = item_data['id']
                    ret.append(iiifManifest(self.config, item_data, self.image_data).manifest())
                elif (item_data['manifest-type'] == 'collection'):
                    ret.append(iiifManifest(self.config, item_data, self.image_data).manifest())
        return ret

    def thumbnail(self):
        return []
        default_page = self.manifest_data['items'][0]

        if 'thumbnail' in self.manifest_data:
            for item in self.manifest_data['items']:
                if item['file'] == self.manifest_data['thumbnail']:
                    default_page = item.copy()

        return [iiifImage(default_page['file'], self.config).thumbnail()]

    def _manifest_id(self):
        if self.type == 'manifest':
            return self.config['manifest-server-base-url'] + '/' + self.id + '/manifest'
        else:
            return self.config['manifest-server-base-url'] + '/collection/' + self.id

    def _convert_metadata(self, metadata):
        ret = []
        if not metadata:
            return ret

        for md in metadata:
            ret.append(self._convert_label_value(md))
        return ret

    def _convert_label_value(self, dict):
        if ('label' in dict and 'value' in dict):
            dict['label'] = self._lang_wrapper(dict['label'])
            dict['value'] = self._lang_wrapper(dict['value'])
            return dict
        return None

    def _find_first_image_for_manifest(self, data):
        if ('items' in data and data['items'][0]['type'] != 'image'):
            return self._find_first_image_for_manifest(data['items'])
        else:
            return data['items'][0]
