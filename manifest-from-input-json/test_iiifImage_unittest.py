import unittest
from iiifImage import iiifImage


class TestIiifImage(unittest.TestCase):
    def setUp(self):
        self.parent_id = 'parent_id'
        self.file_name = 'file_name.tif'
        self.label = 'label'
        self.event_config = {
            "image-server-base-url": "image-server-base-url",
            "manifest-server-base-url": "manifest-server-base-url",
            "canvas-default-height": "200",
            "canvas-default-width": "200"
        }

        self.iiifImage = iiifImage(self.parent_id, self.file_name, self.label, self.event_config)
        pass

    def test_id(self):
        self.assertEqual(self.iiifImage.id, self.parent_id + "%2Ffile_name.tif")

    def test_image_url(self):
        self.assertEqual(self.iiifImage.image_url, "image-server-base-url/parent_id%2Ffile_name.tif")

    def test_filename_without_extension(self):
        self.assertEqual(self.iiifImage.filename_without_extension("file.jpg"), "file")
        self.assertEqual(self.iiifImage.filename_without_extension("file-tif.tif"), "file-tif")
        self.assertEqual(self.iiifImage.filename_without_extension("file.9.jpg"), "file.9")
        self.assertEqual(self.iiifImage.filename_without_extension("file space.jpeg"), "file space")
        self.assertEqual(self.iiifImage.filename_without_extension("f@3516 &343 -1.jpg"), "f@3516 &343 -1")

    def test_thumbnail(self):
        test_thumbnail = {'@id': 'image-server-base-url/parent_id%2Ffile_name.tif/full/250,/0/default.jpg', 'service': {'@id': 'image-server-base-url/parent_id%2Ffile_name.tif', 'profile': 'http://iiif.io/api/image/2/level2.json', '@context': 'http://iiif.io/api/image/2/context.json'}}
        self.assertDictEqual(self.iiifImage.thumbnail(), test_thumbnail)

    def test_image_with_on_canvas(self):
        test_image = {'@id': 'image-server-base-url/parent_id%2Ffile_name.tif', '@type': 'oa:Annotation', 'motivation': 'sc:painting', 'on': 'manifest-server-base-url/parent_id/canvas/parent_id%2Ffile_name.tif', 'resource': {'@id': 'image-server-base-url/parent_id%2Ffile_name.tif/full/full/0/default.jpg', '@type': 'dctypes:Image', 'format': 'image/jpeg', 'service': {'@id': 'image-server-base-url/parent_id%2Ffile_name.tif', 'profile': 'http://iiif.io/api/image/2/level2.json', '@context': 'http://iiif.io/api/image/2/context.json'}}}
        self.assertDictEqual(self.iiifImage.image(), test_image)

    def test_service(self):
        test_service = {'@id': 'image-server-base-url/parent_id%2Ffile_name.tif', 'profile': 'http://iiif.io/api/image/2/level2.json', '@context': 'http://iiif.io/api/image/2/context.json'}
        self.assertDictEqual(self.iiifImage.service(), test_service)

    def test_resource(self):
        test_resource = {'@id': 'image-server-base-url/parent_id%2Ffile_name.tif/full/full/0/default.jpg', '@type': 'dctypes:Image', 'format': 'image/jpeg', 'service': {'@id': 'image-server-base-url/parent_id%2Ffile_name.tif', 'profile': 'http://iiif.io/api/image/2/level2.json', '@context': 'http://iiif.io/api/image/2/context.json'}}
        self.assertDictEqual(self.iiifImage.resource(), test_resource)

    def test_canvas(self):
        test_canvas = {'@id': 'manifest-server-base-url/parent_id/canvas/parent_id%2Ffile_name.tif', '@type': 'sc:Canvas', 'label': 'label', 'height': '200', 'width': '200', 'images': [{'@id': 'image-server-base-url/parent_id%2Ffile_name.tif', '@type': 'oa:Annotation', 'motivation': 'sc:painting', 'on': 'manifest-server-base-url/parent_id/canvas/parent_id%2Ffile_name.tif', 'resource': {'@id': 'image-server-base-url/parent_id%2Ffile_name.tif/full/full/0/default.jpg', '@type': 'dctypes:Image', 'format': 'image/jpeg', 'service': {'@id': 'image-server-base-url/parent_id%2Ffile_name.tif', 'profile': 'http://iiif.io/api/image/2/level2.json', '@context': 'http://iiif.io/api/image/2/context.json'}}}], 'thumbnail': {'@id': 'image-server-base-url/parent_id%2Ffile_name.tif/full/250,/0/default.jpg', 'service': {'@id': 'image-server-base-url/parent_id%2Ffile_name.tif', 'profile': 'http://iiif.io/api/image/2/level2.json', '@context': 'http://iiif.io/api/image/2/context.json'}}}
        self.assertDictEqual(self.iiifImage.canvas(), test_canvas)

if __name__ == '__main__':
    unittest.main()
