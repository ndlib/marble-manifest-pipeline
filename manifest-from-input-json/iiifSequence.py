from iiifImage import iiifImage


class iiifSequence():
    def __init__(self, id, event_config, sequence_data):
        self.id = id
        self.sequence_id = event_config['manifest-server-base-url'] + '/' + self.id + '/sequence/1'
        self.config = event_config
        self.sequence_data = sequence_data

    def sequence(self):
        return {
            '@id': self.sequence_id,
            '@type': 'sc:Sequence',
            'label': self.sequence_data['label'],
            'viewingHint': self.sequence_data['viewingHint'],
            'canvases': self.canvasas()
        }

    def canvasas(self):
        ret = []
        if 'pages' in self.sequence_data:
            for page_data in self.sequence_data['pages']:
                image = iiifImage(self.id, page_data['file'], page_data['label'], self.config)
                ret.append(image.canvas())
        return ret
