import boto3
import csv
from io import StringIO
from pathlib import Path


def load_csv_data(id, config):
    if config['local']:
        return load_id_from_file(id, config)
    else:
        return load_id_from_s3(config['csv-data-files-bucket'], config['csv-data-files-basepath'], id)


def load_id_from_s3(s3Bucket, s3Path, id):
    s3Path = s3Path + "/" + id + ".csv"

    source = boto3.resource('s3').Object(s3Bucket, s3Path)
    source = source.get()['Body'].read().decode('utf-8')
    print(source)
    f = StringIO(source)

    objects = list(csv.DictReader(f, delimiter=','))
    return Item(objects[0], objects).collection()


def load_id_from_file(id, config):
    filepath = config['local_path'] + "csv_data/" + id + ".csv"

    with open(filepath, 'r') as input_source:
        source = input_source.read()
    input_source.close()
    f = StringIO(source)

    objects = list(csv.DictReader(f, delimiter=','))

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
    from pipeline_config import get_pipeline_config
    event = {"local": True}
    config = get_pipeline_config(event)

    # s3 libnd
    config['local'] = False
    for id in ['BPP1001_EAD', 'MSNCOL8500_EAD']:
        parent = load_csv_data(id, config)
        print(parent.get('title'))
        for file in parent.files():
            print(file.get("filePath"))

    # local
    config['local'] = True
    for id in ['parsons', '1976.057']:
        parent = load_csv_data(id, config)
        print(parent.get('title'))
        for file in parent.files():
            print(file.get("filePath"))
