from iiifImage import iiifImage
from iiifCanvas import iiifCanvas
from iiifItem import iiifItem


class iiifManifest(iiifItem):
    def __init__(self, id, config, data, mapping):
        print(config)
        self.config = config
        self.data = data
        self.mapping = mapping
        self.parent_id = id
        self.id = self._make_id()

        iiifItem.__init__(self, self.id, self._schema_to_manifest_type())

    def manifest(self):
        ret = {
            'type': self.type,
            'id': self._manifest_id(),
            'label': self._lang_wrapper(self.data.get('title')),
            'thumbnail': self.thumbnail(),
            'items': self._items(),
            'viewingDirection': 'left-to-right',
            'seeAlso': [{
                "id": self._schema_url(),
                "type": "Dataset",
                "format": "application/ld+json",
                "profile": "https://schema.org/"
            }]
        }

        if self.data.get('repository', False):
            ret['provider'] = self._add_provider(self.data.repository())

        if self.data.get('usage', False):
            ret['requiredStatement'] = self._convert_label_value('Copyright', self.data.get('usage'))

        if self.data .get('license', False):
            ret['rights'] = self.data.get('license')

        if self.data.get('description', False):
            ret['summary'] = self._lang_wrapper(self.data.get('description'))

        if False:
            ret['seeAlso'].append({
                "id": self.config['manifest-server-base-url'] + '/' + self.id + '/mets.xml',
                "type": "Dataset",
                "format": "application/xml",
                "profile": "http://www.loc.gov/METS/"
            })

        metadata = self.metadata_array()
        if len(metadata) > 0:
            ret['metadata'] = self.metadata_array()

        return ret

    def metadata_array(self):
        mapper = self.mapping
        keys_in_other_parts_of_manifest = self.metadata_keys_that_have_top_level_values()

        ret = []
        for key in mapper.get_athena_keys():
            value = self.data.get(key, False)
            label = mapper.get_by_athena(key, 'marble_title')
            if label and value and key not in keys_in_other_parts_of_manifest:
                ret.append(self._convert_label_value(label, self.data.get(key)))

        return ret

    def thumbnail(self):
        if self.data.get('default_image', False):
            return [iiifImage(self.data.get('default_image'), self).thumbnail()]

        thumbnail = self._search_for_default_image(self.data)
        if thumbnail:
            return [iiifImage(thumbnail, self).thumbnail()]

        return []

    def _items(self):
        ret = []
        for item_data in self.data.children():

            if (item_data.type() == 'file'):
                ret.append(iiifCanvas(self, item_data).canvas())
            else:
                ret.append(iiifManifest(self.id, self.config, item_data, self.mapping).manifest())

        return ret

    def _schema_to_manifest_type(self):
        if self.data.type() == 'manifest':
            return 'Manifest'
        elif self.data.type() == 'collection':
            return 'Collection'
        elif self.data.type() == 'file':
            return 'Manifest'

        raise "invalid schema processor type"

    def _manifest_id(self):
        if self.type == 'Manifest':
            print(self.config)
            return self.config['manifest-server-base-url'] + '/' + self.id + '/manifest'
        else:
            return self.config['manifest-server-base-url'] + '/collection/' + self.id

    def _schema_url(self):
        return self.config['manifest-server-base-url'] + '/' + self.id

    def _convert_label_value(self, label, value):
        if (label and value):
            return {
                'label': self._lang_wrapper(label),
                'value': self._lang_wrapper(value)
            }
        return None

    def _add_provider(self, provider):
        if (provider == 'snite'):
            return self._snite_proivider()
        elif (provider == 'archives'):
            return self._archives_proivider()
        elif (provider == 'rbsc'):
            return self._rbsc_proivider()

    def _snite_proivider(self):
        return {
            "id": "https://sniteartmuseum.nd.edu/about-us/contact-us/",
            "type": "Agent",
            "label": {"en": ["Snite Museum of Art"]},
            "homepage": [
                {
                  "id": "https://sniteartmuseum.nd.edu",
                  "type": "Text",
                  "label": {"en": ["Snite Museum of Art"]},
                  "format": "text/html"
                }
            ],
            "logo": [
                {
                  "id": "https://sniteartmuseum.nd.edu/stylesheets/images/snite_logo@2x.png",
                  "type": "Image",
                  "format": "image/png",
                  "height": 100,
                  "width": 120
                }
              ]
        }

    def _rbsc_proivider(self):
        return {
            "id": "https://rarebooks.library.nd.edu/using",
            "type": "Agent",
            "label": {"en": ["Rare Books and Special Collections, Hesburgh Libraries, University of Notre Dame"]},
            "homepage": [
                {
                  "id": "https://rarebooks.library.nd.edu/",
                  "type": "Text",
                  "label": {"en": ["Rare Books and Special Collections"]},
                  "format": "text/html"
                }
            ],
            "logo": [
                {
                  "id": "https://sniteartmuseum.nd.edu/stylesheets/images/snite_logo@2x.png",
                  "type": "Image",
                  "format": "image/png",
                  "height": 100,
                  "width": 120
                }
              ]
        }

    def _archives_proivider(self):
        return {
            "id": "http://archives.nd.edu/about/",
            "type": "Agent",
            "label": {"en": ["University of Notre Dame Archives, Hesburgh Libraries, University of Notre Dame"]},
            "homepage": [
                {
                  "id": "http://archives.nd.edu/",
                  "type": "Text",
                  "label": {"en": ["University of Notre Dame Archives"]},
                  "format": "text/html"
                }
            ],
            "logo": [
                {
                  "id": "https://sniteartmuseum.nd.edu/stylesheets/images/snite_logo@2x.png",
                  "type": "Image",
                  "format": "image/png",
                  "height": 100,
                  "width": 120
                }
              ]
        }

    def _make_id(self):
        if self.parent_id == self.data.get('myId'):
            return self.parent_id

        return self.parent_id + "/" + self.data.get('myId')

    def _search_for_default_image(self, data):
        if data.type() == 'file':
            return data.get('myId')

        for child in data.children():
            if child.type == 'file':
                return child.get('myId')

            return self._search_for_default_image(child)

        return False

    def metadata_keys_that_have_top_level_values(self):
        return [
            'title',
            'provider',
            'description',
            'collectioninformation'
        ]
