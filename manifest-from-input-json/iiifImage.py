import os


class iiifImage():
    def __init__(self, parent_id, file_name, label, event_config):
        self.id = parent_id + '%2F' + self.filename_without_extension(file_name)
        self.file_name = file_name
        self.label = label
        self.image_url = event_config['image-server-base-url'] + '/' + self.id
        self.canvas_url = event_config['manifest-server-base-url'] + '/' + parent_id + '/canvas/' + self.id
        self.config = event_config

    def filename_without_extension(self, file):
        return os.path.splitext(file)[0]

    def thumbnail(self, width="250", height=""):
        return {
            '@id': self.image_url + '/full/' + width + ',' + height + '/0/default.jpg',
            'service': self.service()
        }

    def canvas(self):
        return {
            '@id': self.canvas_url,
            '@type': 'sc:Canvas',
            'label': self.label,
            'height': self.config['canvas-default-height'],
            'width': self.config['canvas-default-width'],
            'images': [
                self.image()
            ],
            'thumbnail': self.thumbnail()
        }

    def image(self):
        return {
            '@id': self.image_url,
            '@type': 'oa:Annotation',
            'motivation': 'sc:painting',
            'on': self.canvas_url,
            'resource': self.resource()
        }

    def resource(self):
        return {
            '@id': self.image_url + '/full/full/0/default.jpg',
            '@type':  'dctypes:Image',
            'format':  'image/jpeg',
            'service':  self.service()
        }

    def service(self):
        return {
            '@id':  self.image_url,
            'profile':  'http://iiif.io/api/image/2/level2.json',
            '@context':  'http://iiif.io/api/image/2/context.json'
        }
