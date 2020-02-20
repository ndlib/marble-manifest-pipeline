from iiifImage import iiifImage


class iiifManifest():
    def __init__(self, config, data, mapping):
        self.config = config
        self.data = data
        self.mapping = mapping
        self.parent_id = self.data.get('collectionId')
        self.id = self.data.get('id')
        self.lang = 'en'
        self.type = self._schema_to_manifest_type()

    def manifest(self):
        ret = {
            'type': self.type,
            'id': self._manifest_id(),
            'label': self._lang_wrapper(self.data.get('title')),
            'thumbnail': self.thumbnail(),
            'items': self._items(),
            'viewingDirection': 'left-to-right'
        }

        if self.key_exists('repository'):
            ret['provider'] = self._add_provider(self.data.get('repository'))

        if self.key_exists('usage'):
            ret['requiredStatement'] = self._convert_label_value('Copyright', self.data.get('usage'))

        if self.key_exists('license'):
            ret['rights'] = self.data.get('license')

        if self.key_exists('description'):
            ret['summary'] = self._lang_wrapper(self.data.get('description'))

        if self.key_exists('height') and self.key_exists('width') and self.type == 'Canvas':
            ret['height'] = self.data.get('height')
            ret['width'] = self.data.get('width')

        if self.key_exists('metsUri'):
            if 'seeAlso' not in ret:
                ret['seeAlso'] = []

            ret['seeAlso'].append({
                "id": self.data.get('metsUri'),
                "type": "Dataset",
                "format": "application/xml",
                "profile": "http://www.loc.gov/METS/"
            })

        if self.key_exists('imageUri'):
            if 'seeAlso' not in ret:
                ret['seeAlso'] = []

            ret['seeAlso'].append({
                "id": self.data.get('schemaUri'),
                "type": "Dataset",
                "format": "application/ld+json",
                "profile": "https://schema.org/"
            })

        metadata = self.metadata_array()
        if len(metadata) > 0:
            ret['metadata'] = self.metadata_array()

        return ret

    def key_exists(self, key):
        return key in self.data.object and self.data.get(key)

    def metadata_array(self):
        mapper = self.mapping
        keys_in_other_parts_of_manifest = self._metadata_keys_that_have_top_level_values()

        ret = []
        for key in mapper.get_athena_keys():
            value = self.data.get(key, False)
            label = mapper.get_by_athena(key, 'marble_title')
            if label and value and key not in keys_in_other_parts_of_manifest:
                ret.append(self._convert_label_value(label, self.data.get(key)))

        return ret

    def thumbnail(self):
        if self.data.get('default_image', False):
            return [iiifImage(self.data).thumbnail()]

        thumbnail = self._search_for_default_image(self.data)
        if thumbnail:
            return [iiifImage(thumbnail).thumbnail()]

        return []

    def _items(self):
        ret = []
        if self.type == 'Canvas':
            image = iiifImage(self.data)
            ret.append({
                'id': self._annotation_page_id(),
                'type': 'AnnotationPage',
                'items': [
                    image.annotation(self._manifest_id())
                ]
            })
        else:
            for item_data in self.data.children():
                ret.append(iiifManifest(self.config, item_data, self.mapping).manifest())

        return ret

    def _schema_to_manifest_type(self):
        if self.data.type() == 'manifest':
            return 'Manifest'
        elif self.data.type() == 'collection':
            return 'Collection'
        elif self.data.type() == 'file':
            return 'Canvas'

        raise "invalid schema processor type"

    def _manifest_id(self):
        return self.data.get('iiifUri')

    def _convert_label_value(self, label, value):
        if (label and value):
            return {
                'label': self._lang_wrapper(label),
                'value': self._lang_wrapper(value)
            }
        return None

    def _lang_wrapper(self, line):
        return {self.lang: [line]}

    def _add_provider(self, provider):
        provider = provider.lower()
        if (provider == 'embark' or provider == 'museum'):
            return self._snite_proivider()
        elif (provider == 'archivesspace'):
            return self._archives_proivider()
        elif (provider == 'rbsc'):
            return self._rbsc_proivider()

        raise Exception("bad provider " + provider.lower())

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
                  "id": "https://rarebooks.library.nd.edu/images/hesburgh_mark.png",
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
                  "id": "https://rarebooks.library.nd.edu/images/hesburgh_mark.png",
                  "type": "Image",
                  "format": "image/png",
                  "height": 100,
                  "width": 120
                }
              ]
        }

    def _annotation_page_id(self):
        return self._manifest_id() + '/annotation_page/' + self.id

    def _search_for_default_image(self, data):
        if data.type() == 'file':
            return data

        for child in data.children():
            if child.type == 'file':
                return child

            return self._search_for_default_image(child)

        return False

    def _metadata_keys_that_have_top_level_values(self):
        return [
            'title',
            'provider',
            'description',
            'collectioninformation',
            'repository'
        ]
