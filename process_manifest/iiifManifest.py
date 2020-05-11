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
        self.manifest_hash = {}
        self._build_mainfest()

    def manifest(self):
        return self.manifest_hash

    def key_exists(self, key):
        return key in self.data.object and self.data.get(key)

    def metadata_array(self):
        mapper = self.mapping
        keys_in_other_parts_of_manifest = self._metadata_keys_that_have_top_level_values()

        ret = []
        for key in mapper.get_athena_keys():
            if key.lower() != 'n/a':
                if key != 'creators':
                    value = self.data.get(key, False)
                else:
                    value = self.data.get(key)
                    if value:
                        value = list(map(lambda row: row.get('display', ''), value))

                label = mapper.get_by_athena(key, 'marble_title')
                if label and value and key not in keys_in_other_parts_of_manifest:
                    ret.append(self._convert_label_value(label, value))

        return ret

    def thumbnail(self):
        if self.data.get('default_image', False):
            return [iiifImage(self.data).thumbnail()]

        thumbnail = self._search_for_default_image(self.data)
        if thumbnail:
            return [iiifImage(thumbnail).thumbnail()]

        return []

    def _build_mainfest(self):
        self.manifest_hash = {
            'type': self.type,
            'id': self._manifest_id(),
            'label': self._lang_wrapper(self.data.get('title')),
            'thumbnail': self.thumbnail(),
            'items': self._items(),
            'viewingDirection': 'left-to-right'
        }

        self.add_context()
        self.add_provider()
        self.add_required_statement()
        self.add_license()
        self.add_description()
        self.add_width_height()
        self.add_mets()
        self.add_schema_org()
        self.add_pdf()

        metadata = self.metadata_array()
        if len(metadata) > 0:
            self.manifest_hash['metadata'] = metadata

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
                if item_data.get('mimeType') != 'application/pdf':
                    ret.append(iiifManifest(self.config, item_data, self.mapping).manifest())

        return ret

    def _schema_to_manifest_type(self):
        if self.data.type() == 'manifest':
            return 'Manifest'
        elif self.data.type() == 'collection':
            return 'Collection'
        elif self.data.type() == 'file':
            return 'Canvas'

        raise Exception("invalid schema processor type: " + self.data.type())

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
        if type(line) != list:
            line = [line]

        line = list(map(lambda x: str(x), iter(line)))
        return {self.lang: line}

    def add_context(self):
        if self.data.root():
            self.manifest_hash["@context"] = [
                "http://www.w3.org/ns/anno.jsonld",
                "https://presentation-iiif.library.nd.edu/extensions/partiallyDigitized",
                "http://iiif.io/api/presentation/3/context.json"
            ]

    def add_schema_org(self):
        if self.key_exists('schemaUri'):
            if 'seeAlso' not in self.manifest_hash:
                self.manifest_hash['seeAlso'] = []

            self.manifest_hash['seeAlso'].append({
                "id": self.data.get('schemaUri'),
                "type": "Dataset",
                "format": "application/ld+json",
                "profile": "https://schema.org/"
            })

    def add_mets(self):
        if self.key_exists('metsUri'):
            if 'seeAlso' not in self.manifest_hash:
                self.manifest_hash['seeAlso'] = []

            self.manifest_hash['seeAlso'].append({
                "id": self.data.get('metsUri'),
                "type": "Dataset",
                "profile": "http://www.loc.gov/METS/",
                "format": "application/xml",
            })

    def add_pdf(self):
        # if we are a manifest and we have a child that is a pdf add them to a render section.
        if self.type == 'Manifest':
            pdfs = []
            for item_data in self.data.children():
                if item_data.get('mimeType') == 'application/pdf':
                    pdfs.append({
                        "id": item_data.get("filePath"),
                        "type": "Text",
                        "label": {"en": ["PDF Rendering"]},
                        "format": "application/pdf"
                    })

            if len(pdfs) > 0:
                self.manifest_hash['rendering'] = pdfs

    def add_width_height(self):
        if self.key_exists('height') and self.key_exists('width') and self.type == 'Canvas':
            self.manifest_hash['height'] = self.data.get('height')
            self.manifest_hash['width'] = self.data.get('width')

    def add_description(self):
        if self.key_exists('description'):
            self.manifest_hash['summary'] = self._lang_wrapper(self.data.get('description'))

    def add_license(self):
        if self.key_exists('copyrightStatement'):
            self.manifest_hash['rights'] = self.data.get('copyrightStatement')

    def add_required_statement(self):
        if self.key_exists('copyrightStatus'):
            self.manifest_hash['requiredStatement'] = self._convert_label_value('Copyright', self.data.get('copyrightStatus'))

    def add_provider(self):
        if not self.key_exists('repository'):
            return

        if self.data.type() == 'file':
            return

        provider = self.data.get('repository').lower()
        if (provider == 'embark' or provider == 'museum'):
            self.manifest_hash['provider'] = [self._snite_proivider()]
        elif (provider == 'unda'):
            self.manifest_hash['provider'] = [self._archives_proivider()]
        elif (provider == 'rbsc' or provider == 'rare' or provider == 'spec' or provider == 'mrare'):
            self.manifest_hash['provider'] = [self._rbsc_proivider()]
        else:
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
            'creator',
            'description',
            'collectioninformation',
            'repository',
            'copyrightStatus',
            'copyrightStatement',
            'usage',
            'license',
            'thumbnail',
        ]
