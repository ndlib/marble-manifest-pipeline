from iiifItem import iiifItem
from iiifImage import iiifImage
from iiifAnnotationPage import iiifAnnotationPage


class iiifCanvas(iiifItem):
    def __init__(self, id, label, config):
        iiifItem.__init__(self, id, 'Canvas')
        self.config = config
        self.label = label

    def canvas(self):
        return {
            'id': self._canvas_id(),
            'type': self.type,
            'label': super().label_wrapper(self.label),
            'height': self.config['canvas-default-height'],
            'width': self.config['canvas-default-width'],
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
