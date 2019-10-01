from iiifItem import iiifItem


class iiifImage(iiifItem):
    def __init__(self, id, event_config):
        iiifItem.__init__(self, id, 'Image')
        self.config = event_config

    def thumbnail(self, width="250", height=""):
        return {
            'id': self._image_id() + '/full/' + width + ',' + height + '/0/default.jpg',
            'type': 'Image',
            'service': [
                self._service()
            ]
        }

    def image(self):
        return {
            'id': self._image_id() + '/full/full/0/default.jpg',
            'type': 'Image',
            'format': 'image/jpeg',
            'service': [self._service()]
        }

    def _service(self):
        return {
            'id':  self._image_id(),
            'type': 'ImageService2',
            'profile': "http://iiif.io/api/image/2/level2.json"
        }

    def _image_id(self):
        return self.config['image-server-base-url'] + '/' + self.id
