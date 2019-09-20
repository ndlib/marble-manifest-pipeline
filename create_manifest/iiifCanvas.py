from create_manifest.iiifItem import iiifItem
from create_manifest.iiifImage import iiifImage
from create_manifest.iiifAnnotationPage import iiifAnnotationPage


class iiifCanvas(iiifItem):
    def __init__(self, item_data, config):
        # item_data = {'file': '1982_072_001_a-v0001.jpg', 'label': '072_001_a-v0001', 'height': 1747, 'width': 3000}
        iiifItem.__init__(self, item_data['file'], 'Canvas')
        self.config = config
        self.label = self._lang_wrapper(item_data.get('label', item_data['file']))
        self.height = item_data['height']
        self.width = item_data['width']

    def canvas(self):
        return {
            'id': self._canvas_id(),
            'type': self.type,
            'label': self.label,
            'height': self.height,
            'width': self.width,
            'thumbnail': [
                iiifImage(self.id, self.config).thumbnail()
            ],
            'items': [
                iiifAnnotationPage(self.id, self.config).page()
            ]
        }

    def _canvas_id(self):
        return self.config['manifest-server-base-url'] + '/' + self.config['event_id'] \
            + '/canvas/' + self.config['event_id'] + '%2F' + self.id
