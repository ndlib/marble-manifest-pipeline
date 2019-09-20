from manifest_from_input_json.iiifItem import iiifItem
from manifest_from_input_json.iiifImage import iiifImage


class iiifAnnotation(iiifItem):
    def __init__(self, id, config):
        iiifItem.__init__(self, id, 'Annotation')
        self.config = config

    def annotation(self):
        return {
            'id': self._annotation_id(),
            'type': self.type,
            'motivation': 'painting',
            'target': self._target_id(),
            'body': iiifImage(self.id, self.config).image()
        }

    def _annotation_id(self):
        return self.config['image-server-base-url'] + '/' + self.config['event_id'] \
            + '%2F' + super().filename_with_tif_extension(self.id)

    def _target_id(self):
        return self.config['manifest-server-base-url'] + '/' + self.config['event_id'] \
            + '/canvas/' + self.config['event_id'] + '%2F' + super().filename_with_tif_extension(self.id)
