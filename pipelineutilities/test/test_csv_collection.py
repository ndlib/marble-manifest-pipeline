import sys
import os
import unittest
where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am + "/../")
from pipelineutilities.csv_collection import Item

objects = [
    {"sourceSystem": "EmbARK", "repository": "repository", "myId": "collectionId", "collectionId": "collectionId", "parentId": "root", "level": "collection"},
    {"sourceSystem": "", "repository": "", "myId": "itemId", "collectionId": "collectionId", "parentId": "collectionId", "level": "manifest"},
    {"sourceSystem": "", "repository": "", "myId": "fileId1", "collectionId": "collectionId", "parentId": "itemId", "level": "manifest"},
    {"sourceSystem": "", "repository": "", "myId": "fileId2", "collectionId": "collectionId", "parentId": "itemId", "level": "manifest"}
]


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
