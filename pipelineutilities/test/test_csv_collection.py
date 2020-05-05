import _set_path  # noqa
import unittest
from pipelineutilities.csv_collection import Item, _check_creator, _add_additional_paths, _add_image_dimensions, _turn_strings_to_json, _fix_ids  # noqa: E402

objects = [
    {"sourceSystem": "EmbARK", "repository": "repository", "id": "collectionId", "collectionId": "collectionId", "parentId": "root", "level": "collection"},
    {"sourceSystem": "", "repository": "", "id": "itemId", "collectionId": "collectionId", "parentId": "collectionId", "level": "manifest", "creator": "", "creators": "[]"},
    {"sourceSystem": "", "repository": "", "id": "fileId1", "collectionId": "collectionId", "parentId": "itemId", "level": "file"},
    {"sourceSystem": "", "repository": "", "id": "fileId2", "collectionId": "collectionId", "parentId": "itemId", "level": "file", "creators": ""},
    {"sourceSystem": "", "repository": "", "id": "itemId", "collectionId": "collectionId", "parentId": "collectionId", "level": "manifest", "creator": ""},
    {"sourceSystem": "", "repository": "", "id": "fileId2", "collectionId": "collectionId", "parentId": "itemId", "level": "manifest", "creators": '[{ "fullName": "Bob Bobbers" }]'},
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

    def test_converts_json_fields(self):
        test = {'somejson': '[{"obj": "value"}]'}
        _turn_strings_to_json(test)
        self.assertEqual(test['somejson'], [{"obj": "value"}])

        test = {'somejson': '[{}]'}
        _turn_strings_to_json(test)
        self.assertEqual(test['somejson'], [{}])

        test = {'somejson': '{}'}
        _turn_strings_to_json(test)
        self.assertEqual(test['somejson'], {})

        test = {'somearrrayjson': '["obj"]'}
        _turn_strings_to_json(test)
        self.assertEqual(test['somearrrayjson'], ["obj"])

        test = {'somearrrayjson': '[]'}
        _turn_strings_to_json(test)
        self.assertEqual(test['somearrrayjson'], [])

    def test_fix_ids(self):
        # converts these to floats
        test = {"id": 1998.34, "collectionId": 1442.23, "parentId": 2344.12}
        _fix_ids(test)
        self.assertEqual(test, {"id": "1998.34", "collectionId": "1442.23", "parentId": "2344.12"})

        # does not convert other floats
        test["other"] = 2342.32
        _fix_ids(test)
        self.assertEqual(test["other"], 2342.32)

    def test_check_creator(self):
        # if there is a creators it keeps it no matter the level
        test = {"creators": [{"fullName": "Bob Bobbers"}], "level": "collection"}
        _check_creator(test)
        self.assertEqual([{"fullName": "Bob Bobbers"}], test['creators'])

        test = {"creators": [{"fullName": "Bob Bobbers"}], "level": "manifest"}
        _check_creator(test)
        self.assertEqual([{"fullName": "Bob Bobbers"}], test['creators'])

        test = {"creators": [{"fullName": "Bob Bobbers"}], "level": "file"}
        _check_creator(test)
        self.assertEqual([{"fullName": "Bob Bobbers"}], test['creators'])

        # if there is no creators key at all set it if it is manifest or collection
        test = {"level": "collection"}
        _check_creator(test)
        self.assertEqual([{"fullName": "unknown"}], test['creators'])

        test = {"level": "manifest"}
        _check_creator(test)
        self.assertEqual([{"fullName": "unknown"}], test['creators'])

        # but not if it is a file
        test = {"level": "file"}
        _check_creator(test)
        self.assertEqual(None, test.get('creators', None))

        # if the creators key is empty set it if it is a collection or manifest
        test = {"level": "collection", "creators": ""}
        _check_creator(test)
        self.assertEqual([{"fullName": "unknown"}], test['creators'])

        test = {"level": "manifest", "creators": ""}
        _check_creator(test)
        self.assertEqual([{"fullName": "unknown"}], test['creators'])

        # but not if it is a file
        test = {"level": "file", "creators": ""}
        _check_creator(test)
        self.assertEqual("", test['creators'])

        # if the creators key is empty list it if it is a collection or manifest
        test = {"level": "collection", "creators": []}
        _check_creator(test)
        self.assertEqual([{"fullName": "unknown"}], test['creators'])

        test = {"level": "manifest", "creators": []}
        _check_creator(test)
        self.assertEqual([{"fullName": "unknown"}], test['creators'])

        # but not if it is a file
        test = {"level": "file", "creators": []}
        _check_creator(test)
        self.assertEqual([], test['creators'])

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
        self.assertEqual("s3://manifest-server-bucket/collectionId/itemId/manifest", objects[1]['iiifFilePath'])
        self.assertEqual("manifest-server-base-url/collectionId/itemId/mets.xml", objects[1]['metsUri'])
        self.assertEqual("s3://manifest-server-bucket/collectionId/itemId/mets.xml", objects[1]['metsFilePath'])
        self.assertEqual("manifest-server-base-url/collectionId/itemId", objects[1]['schemaUri'])
        self.assertEqual("s3://manifest-server-bucket/collectionId/itemId/index.json", objects[1]['schemaPath'])

    def test_add_additional_paths_for_collections(self):
        _add_additional_paths(objects[0], config)

        self.assertEqual("", objects[0]['iiifImageUri'])
        self.assertEqual("", objects[0]['iiifImageFilePath'])
        self.assertEqual("manifest-server-base-url/collectionId/manifest", objects[0]['iiifUri'])
        self.assertEqual("s3://manifest-server-bucket/collectionId/manifest", objects[0]['iiifFilePath'])
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
