from s3_helpers import read_s3_json
import os
import boto3
import json


def load_nd_json(id, config):
    if config.get('local', False):
        tree = _load_json_from_file(id, config['local-path'])
    else:
        tree = _load_json_from_s3(config['process-bucket'], 'json', id)
    return Item(tree, tree)


def _load_json_from_file(id, local_path):
    filepath = os.path.join(local_path, 'json', id + ".json")
    try:
        with open(filepath, 'r') as input_source:
            source = input_source.read()
        input_source.close()
    except FileNotFoundError:
        return {}

    return json.loads(source)


def _load_json_from_s3(s3_bucket, s3_path, id):
    s3_path = os.path.join(s3_path, id + ".json")

    print(s3_bucket, s3_path)
    try:
        return read_s3_json(s3_bucket, s3_path)
    except boto3.resource('s3').meta.client.exceptions.NoSuchKey:
        return {}


def _flatten_dict(indict, pre=None):
    pre = pre[:] if pre else []
    if isinstance(indict, dict):
        for key, value in indict.items():
            if isinstance(value, dict):
                for d in _flatten_dict(value, pre + [key]):
                    yield d
            elif isinstance(value, list) or isinstance(value, tuple):
                for v in value:
                    for d in _flatten_dict(v, pre + [key]):
                        yield d
            else:
                yield pre + [key, value]
    else:
        yield pre + [indict]


class Item():

    def __init__(self, object, all_objects, parent=None):
        self.parent = parent
        self.object = object
        self.all_objects = all_objects

    def repository(self):
        return self.collection().get('sourceSystem', 'aleph')

    def type(self):
        return self.get('level')

    def collection(self):
        return Item(self.all_objects, self.all_objects)

    def root(self):
        return self.get("parentId", False) == "root"

    def get(self, key, default=False):
        return self.object.get(key, default)

    def children(self) -> list:
        return map(lambda item: Item(item, self.all_objects, self.object), self.object.get("items", []))

    def files(self) -> list:
        ret = []

        for child in self.children():
            if child.type() == 'file':
                ret.append(child)
            else:
                ret = ret + child.files()

        return ret

    def _find_row(self, id, parent):
        for this_row in parent.children():
            if this_row.get('id', False) == id:
                return Item(this_row, self.all_objects)

        return False


# python -c 'from load_nd_json import *; test()'
def test():
    from pipeline_config import setup_pipeline_config
    event = {"local": True}
    event['local-path'] = '../../example/'

    event['ssm_key_base'] = "/all/marble-manifest-deployment/prod"
    config = setup_pipeline_config(event)
    object = load_nd_json('1951.004.009', config)
    print(list(object.children()))
