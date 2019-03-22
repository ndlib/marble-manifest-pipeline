import unittest
import os
from iiifSequence import iiifSequence


class TestIiifImage(unittest.TestCase):
    def setUp(self):
        self.parent_id = 'parent_id'
        self.event_config = {
            "image-server-base-url": "image-server-base-url",
            "manifest-server-base-url": "manifest-server-base-url",
            "canvas-default-height": "200",
            "canvas-default-width": "200"
        }

        self.sequence_data = {
            "label": "sequence",
            "viewingHint": "individuals",
            "pages": [
                {"file": "file1.jpg", "label": "file1"},
                {"file": "file2.tif", "label": "file2"},
            ]
        }

        self.iiifSequence = iiifSequence(self.parent_id, self.event_config, self.sequence_data)
        pass

    def test_id(self):
        self.assertEqual(self.iiifSequence.id, self.parent_id)

    def test_sequence_id(self):
        self.assertEqual(self.iiifSequence.sequence_id, os.path.join(self.event_config['manifest-server-base-url'], self.parent_id, 'sequence', '1'))

    def test_config(self):
        self.assertDictEqual(self.iiifSequence.config, self.event_config)

    def test_sequence_data(self):
        self.assertDictEqual(self.iiifSequence.sequence_data, self.sequence_data)

    def test_canvasas(self):
        test = [{'@id': 'manifest-server-base-url/parent_id/canvas/parent_id%2Ffile1', '@type': 'sc:Canvas', 'label': 'file1', 'height': '200', 'width': '200', 'images': [{'@id': 'image-server-base-url/parent_id%2Ffile1', '@type': 'oa:Annotation', 'motivation': 'sc:painting', 'on': 'manifest-server-base-url/parent_id/canvas/parent_id%2Ffile1', 'resource': {'@id': 'image-server-base-url/parent_id%2Ffile1/full/full/0/default.jpg', '@type': 'dctypes:Image', 'format': 'image/jpeg', 'service': {'@id': 'image-server-base-url/parent_id%2Ffile1', 'profile': 'http://iiif.io/api/image/2/level2.json', '@context': 'http://iiif.io/api/image/2/context.json'}}}], 'thumbnail': {'@id': 'image-server-base-url/parent_id%2Ffile1/full/250,/0/default.jpg', 'service': {'@id': 'image-server-base-url/parent_id%2Ffile1', 'profile': 'http://iiif.io/api/image/2/level2.json', '@context': 'http://iiif.io/api/image/2/context.json'}}}, {'@id': 'manifest-server-base-url/parent_id/canvas/parent_id%2Ffile2', '@type': 'sc:Canvas', 'label': 'file2', 'height': '200', 'width': '200', 'images': [{'@id': 'image-server-base-url/parent_id%2Ffile2', '@type': 'oa:Annotation', 'motivation': 'sc:painting', 'on': 'manifest-server-base-url/parent_id/canvas/parent_id%2Ffile2', 'resource': {'@id': 'image-server-base-url/parent_id%2Ffile2/full/full/0/default.jpg', '@type': 'dctypes:Image', 'format': 'image/jpeg', 'service': {'@id': 'image-server-base-url/parent_id%2Ffile2', 'profile': 'http://iiif.io/api/image/2/level2.json', '@context': 'http://iiif.io/api/image/2/context.json'}}}], 'thumbnail': {'@id': 'image-server-base-url/parent_id%2Ffile2/full/250,/0/default.jpg', 'service': {'@id': 'image-server-base-url/parent_id%2Ffile2', 'profile': 'http://iiif.io/api/image/2/level2.json', '@context': 'http://iiif.io/api/image/2/context.json'}}}]
        self.assertListEqual(self.iiifSequence.canvasas(), test)

    def test_sequence(self):
        test = {'@id': 'manifest-server-base-url/parent_id/sequence/1', '@type': 'sc:Sequence', 'label': 'sequence', 'viewingHint': 'individuals', 'canvases': [{'@id': 'manifest-server-base-url/parent_id/canvas/parent_id%2Ffile1', '@type': 'sc:Canvas', 'label': 'file1', 'height': '200', 'width': '200', 'images': [{'@id': 'image-server-base-url/parent_id%2Ffile1', '@type': 'oa:Annotation', 'motivation': 'sc:painting', 'on': 'manifest-server-base-url/parent_id/canvas/parent_id%2Ffile1', 'resource': {'@id': 'image-server-base-url/parent_id%2Ffile1/full/full/0/default.jpg', '@type': 'dctypes:Image', 'format': 'image/jpeg', 'service': {'@id': 'image-server-base-url/parent_id%2Ffile1', 'profile': 'http://iiif.io/api/image/2/level2.json', '@context': 'http://iiif.io/api/image/2/context.json'}}}], 'thumbnail': {'@id': 'image-server-base-url/parent_id%2Ffile1/full/250,/0/default.jpg', 'service': {'@id': 'image-server-base-url/parent_id%2Ffile1', 'profile': 'http://iiif.io/api/image/2/level2.json', '@context': 'http://iiif.io/api/image/2/context.json'}}}, {'@id': 'manifest-server-base-url/parent_id/canvas/parent_id%2Ffile2', '@type': 'sc:Canvas', 'label': 'file2', 'height': '200', 'width': '200', 'images': [{'@id': 'image-server-base-url/parent_id%2Ffile2', '@type': 'oa:Annotation', 'motivation': 'sc:painting', 'on': 'manifest-server-base-url/parent_id/canvas/parent_id%2Ffile2', 'resource': {'@id': 'image-server-base-url/parent_id%2Ffile2/full/full/0/default.jpg', '@type': 'dctypes:Image', 'format': 'image/jpeg', 'service': {'@id': 'image-server-base-url/parent_id%2Ffile2', 'profile': 'http://iiif.io/api/image/2/level2.json', '@context': 'http://iiif.io/api/image/2/context.json'}}}], 'thumbnail': {'@id': 'image-server-base-url/parent_id%2Ffile2/full/250,/0/default.jpg', 'service': {'@id': 'image-server-base-url/parent_id%2Ffile2', 'profile': 'http://iiif.io/api/image/2/level2.json', '@context': 'http://iiif.io/api/image/2/context.json'}}}]}
        self.assertDictEqual(self.iiifSequence.sequence(), test)


if __name__ == '__main__':
    unittest.main()
