from iiifItem import iiifItem
from iiifImage import iiifImage


class iiifCanvas(iiifItem):
    def __init__(self, manifest, item_data):
        # item_data = {'file': '1982_072_001_a-v0001.jpg', 'label': '072_001_a-v0001', 'height': 1747, 'width': 3000}
        iiifItem.__init__(self, item_data.get('myId'), 'Canvas')
        self.manifest = manifest
        self.label = self._lang_wrapper(item_data.get('label', item_data.get('title')))
        self.height = item_data.get('width', 2000)
        self.width = item_data.get('height', 2000)
        self.item_data = item_data
        # myId is currently the filename
        self.image = iiifImage(item_data.get('myId'), self.manifest)

    def canvas(self):
        return {
            'id': self.canvas_url_id(),
            'type': self.type,
            'label': self.label,
            'height': self.height,
            'width': self.width,
            'thumbnail': [
                self.image.thumbnail()
            ],
            'items': [
                self.annotation_page()
            ]
        }

    def annotation_page(self):
        return {
            'id': self._annotation_page_id(),
            'type': 'AnnotationPage',
            'items': [
                self.annotation()
            ]
        }

    def annotation(self):
        return {
            'id': self._annotation_id(),
            'type': 'Annotation',
            'motivation': 'painting',
            'target': self.canvas_url_id(),
            'body': self.image.image()
        }

    def canvas_url_id(self):
        return self.manifest.config['manifest-server-base-url'] + '/' + self.manifest.parent_id \
            + '/canvas/' + self.id

    def _annotation_page_id(self):
        return self.manifest.config['manifest-server-base-url'] + '/' + self.manifest.parent_id \
            + '/annotation_page/' + self.id

    def _annotation_id(self):
        return self.manifest.config['manifest-server-base-url'] + '/' + self.manifest.parent_id \
            + '/annotation/' + self.id
