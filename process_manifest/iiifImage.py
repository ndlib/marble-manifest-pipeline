from iiifItem import iiifItem
import os


class iiifImage(iiifItem):
    def __init__(self, filename, manifest):
        self.filename = filename
        self.manifest = manifest

    def thumbnail(self, width="250", height=""):
        return {
            'id': self.image_url_id() + '/full/' + width + ',' + height + '/0/default.jpg',
            'type': 'Image',
            'service': [
                self._service()
            ]
        }

    def image(self):
        return {
            'id': self.image_url_id() + '/full/full/0/default.jpg',
            'type': 'Image',
            'format': 'image/jpeg',
            'service': [self._service()]
        }

    def _service(self):
        return {
            'id': self.image_url_id(),
            'type': 'ImageService2',
            'profile': "http://iiif.io/api/image/2/level2.json"
        }

    def image_url_id(self):
        path = os.path.join('%2F', self.manifest.parent_id, os.path.splitext(self.filename)[0])
        return self.manifest.config['image-server-base-url'] + path
