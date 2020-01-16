import boto3
import csv
import json
from io import StringIO
from pathlib import Path


def load_id_from_s3(s3Bucket, s3Path, id):
    s3Path = s3Path + "/" + id + ".csv"
    source = read_s3_file_content(s3Bucket, s3Path)
    f = StringIO(source)

    objects = list(csv.DictReader(f, delimiter=','))
    return Item(objects[0], objects).collection()


def read_s3_file_content(s3Bucket, s3Path):
    content_object = boto3.resource('s3').Object(s3Bucket, s3Path)
    return content_object.get()['Body'].read().decode('utf-8')


def load_csv_from_file_system(filepath):
    with open(filepath, 'r') as input_source:
        source = input_source.read()
    input_source.close()
    f = StringIO(source)
    return csv.DictReader(f, delimiter=',')


def load_json_from_file_system(filepath):
    with open(filepath, 'r') as input_source:
        source = json.loads(input_source.read())
    input_source.close()
    return source


def load_id_from_file(path, id):
    current_path = str(Path(__file__).parent.absolute())
    filepath = current_path + "/" + path + "/" + id + ".csv"

    objects = list(load_csv_from_file_system(filepath))
    image_data = load_json_from_file_system(current_path + "/" + path + "/image-data.json")
    for object in objects:
        if object.get('filepath', False) and object.get('filepath'):
            key = object.get('myId')
            object['width'] = image_data[key].get('width', 2000)
            object['height'] = image_data[key].get('height', 2000)
            object['etag'] = image_data[key].get('etag', '')
        else:
            object['width'] = ''
            object['height'] = ''
            object['etag'] = ''

    return Item(objects[0], objects).collection()


class Item():

    def __init__(self, object, all_objects):
        self.object = object
        self.all_objects = all_objects

    def repository(self):
        return 'snite'

    def type(self):
        return self.get('level')

    def collection(self):
        return self._find_row(self.object.get('collectionId'))

    def parent(self):
        return self._find_row(self.object.get('parentId'))

    def get(self, key, default=False):
        return self.object.get(key, default)

    def children(self):
        children = []
        test_id = "".join(self.get('myId').lower().split(" "))
        for row in self.all_objects:
            if "".join(row['parentId'].lower().split(" ")) == test_id:
                children.append(Item(row, self.all_objects))

        return children

    def files(self):
        ret = []

        for child in self.children():
            if child.type() == 'file':
                ret.append(child)
            else:
                ret = ret + child.files()

        return ret

    def _find_row(self, id):
        for this_row in self.all_objects:
            if this_row.get('myId', False) == id:
                return Item(this_row, self.all_objects)


# python -c 'from csv_collection import *; test()'
def test():
    # s3 libnd
    bucket = ""
    path = "archives-space-csv-files"

    for id in ['BPP1001_EAD', 'MSNCOL8500_EAD']:
        parent = load_id_from_s3(bucket, path, id)
        print(parent.get('title'))
        for file in parent.files():
            print(file.get("filePath"))

    # local
    for id in ['parsons2']:
        parent = load_id_from_file('../example/item-one-image', id)
        print(parent)
