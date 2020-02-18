import sys
import os
import unittest
where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am + "/../")
from pipelineutilities.csv_collection import Item, _check_creator, _add_additional_paths, _add_image_dimensions

objects = [
    {"sourceSystem": "EmbARK", "repository": "repository", "id": "collectionId", "collectionId": "collectionId", "parentId": "root", "level": "collection"},
    {"sourceSystem": "", "repository": "", "id": "itemId", "collectionId": "collectionId", "parentId": "collectionId", "level": "manifest", "creator": ""},
    {"sourceSystem": "", "repository": "", "id": "fileId1", "collectionId": "collectionId", "parentId": "itemId", "level": "file"},
    {"sourceSystem": "", "repository": "", "id": "fileId2", "collectionId": "collectionId", "parentId": "itemId", "level": "file", "creator": "Bob Bobbers"}
]

config = {
    "image-server-base-url": "image-server-base-url",
    "image-server-bucket": "image-server-bucket",
    "manifest-server-base-url": "manifest-server-base-url",
    "manifest-server-bucket": "manifest-server-bucket",
    "canvas-default-height": 300,
    "canvas-default-width": 400
}

all_image_data = {
    "fileId1": {
        "height": 100,
        "width": 200
    }
}

class TestCsvCollection(unittest.TestCase):

    def test_collection(self):
        parent = Item(objects[0], objects)
        self.assertEqual(parent.collection().object, objects[0])

    def test_children(self):
        parent = Item(objects[0], objects)
        children = parent.children()

        self.assertEqual(children[0].object, objects[1])

        file_children = children[0].children()
        self.assertEqual(file_children[0].object, objects[2])
        self.assertEqual(file_children[1].object, objects[3])

    def test_files(self):
        parent = Item(objects[0], objects)
        files = parent.files()

        self.assertEqual(files[0].object, objects[2])
        self.assertEqual(files[1].object, objects[3])

    def test_check_creator(self):
        # if there is a creator it keeps it
        _check_creator(objects[3])
        self.assertEqual("Bob Bobbers", objects[3]['creator'])

        # if there is no creator key it sets it to unknown
        _check_creator(objects[2])
        self.assertEqual("unknown", objects[2]['creator'])

        # if the key is empty it sets it to unknown
        _check_creator(objects[1])
        self.assertEqual("unknown", objects[1]['creator'])

    def test_add_additional_paths_for_files(self):
        _add_additional_paths(objects[2], config)

        self.assertEqual("image-server-base-url/collectionId%2FfileId1", objects[2]['iiifImageUri'])
        self.assertEqual("s3://image-server-bucket/collectionId/fileId1", objects[2]['iiifImageFilePath'])
        self.assertEqual("manifest-server-base-url/collectionId/fileId1/canvas", objects[2]['iiifUri'])
        self.assertEqual("s3://manifest-server-bucket/collectionId/fileId1/canvas/index.json", objects[2]['iiifFilePath'])
        self.assertEqual("", objects[2]['metsUri'])
        self.assertEqual("", objects[2]['metsFilePath'])
        self.assertEqual("", objects[2]['schemaUri'])
        self.assertEqual("", objects[2]['schemaPath'])

    def test_add_additional_paths_for_manifests(self):
        _add_additional_paths(objects[1], config)

        self.assertEqual("", objects[1]['iiifImageUri'])
        self.assertEqual("", objects[1]['iiifImageFilePath'])
        self.assertEqual("manifest-server-base-url/collectionId/itemId/manifest", objects[1]['iiifUri'])
        self.assertEqual("s3://manifest-server-bucket/collectionId/itemId/manifest/index.json", objects[1]['iiifFilePath'])
        self.assertEqual("manifest-server-base-url/collectionId/itemId/mets.xml", objects[1]['metsUri'])
        self.assertEqual("s3://manifest-server-bucket/collectionId/itemId/mets.xml", objects[1]['metsFilePath'])
        self.assertEqual("manifest-server-base-url/collectionId/itemId", objects[1]['schemaUri'])
        self.assertEqual("s3://manifest-server-bucket/collectionId/itemId/index.json", objects[1]['schemaPath'])

    def test_add_additional_paths_for_collections(self):
        _add_additional_paths(objects[0], config)

        self.assertEqual("", objects[0]['iiifImageUri'])
        self.assertEqual("", objects[0]['iiifImageFilePath'])
        self.assertEqual("manifest-server-base-url/collectionId/manifest", objects[0]['iiifUri'])
        self.assertEqual("s3://manifest-server-bucket/collectionId/manifest/index.json", objects[0]['iiifFilePath'])
        self.assertEqual("manifest-server-base-url/collectionId/mets.xml", objects[0]['metsUri'])
        self.assertEqual("s3://manifest-server-bucket/collectionId/mets.xml", objects[0]['metsFilePath'])
        self.assertEqual("manifest-server-base-url/collectionId", objects[0]['schemaUri'])
        self.assertEqual("s3://manifest-server-bucket/collectionId/index.json", objects[0]['schemaPath'])

    def test_add_image_dimensions(self):
        # if the height and width is in the image_data it pulls that
        _add_image_dimensions(objects[2], all_image_data, config)

        self.assertEqual(100, objects[2]['height'])
        self.assertEqual(200, objects[2]['width'])

        # if the height and width is in the image_data it pulls that
        _add_image_dimensions(objects[3], all_image_data, config)

        self.assertEqual(300, objects[3]['height'])
        self.assertEqual(400, objects[3]['width'])


if __name__ == '__main__':
    unittest.main()
