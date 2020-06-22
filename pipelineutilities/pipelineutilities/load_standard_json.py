from s3_helpers import read_s3_json
import os
import boto3
import json


def load_image_data(id, config):
    if config.get('local', False):
        return load_image_from_file(id, config)
    else:
        return load_image_from_s3(config['process-bucket'], config['process-bucket-read-basepath'], id, config)


def load_image_from_file(id, config):
    filepath = config['local-path'] + "/" + id + "/" + config['image-data-file']
    try:
        with open(filepath, 'r') as input_source:
            source = input_source.read()
        input_source.close()
    except FileNotFoundError:
        return {}

    return json.loads(source)


def load_image_from_s3(s3Bucket, s3Path, id, config):
    s3Path = s3Path + "/" + id + "/" + config['image-data-file']

    try:
        content_object = boto3.resource('s3').Object(s3Bucket, s3Path)
        source = content_object.get()['Body'].read().decode('utf-8')
        return json.loads(source)
    except boto3.resource('s3').meta.client.exceptions.NoSuchKey:
        return {}


def load_standard_json(id, config):
    if config.get('local', False):
        tree = _load_json_from_file(id, config['local-path'])
    else:
        tree = _load_json_from_s3(config['process-bucket'], config['process-bucket-data-basepath'], id)

    images = load_image_data(id, config)
    _augment_row_data(tree, images, config)

    return Item(tree, tree)


def _augment_row_data(tree, all_image_data, config):
    _add_image_dimensions(tree, all_image_data, config)

    for item in tree.get('items', []):
        _augment_row_data(item, all_image_data, config)


def _add_image_dimensions(row, all_image_data, config):
    level = row.get('level')
    if level != "file":
        row.update({"width": None, "height": None})
        return

    image_key = os.path.splitext(row.get('id'))[0]
    image_data = all_image_data.get(image_key, {})

    ret = {
        "height": image_data.get('height', False),
        "width": image_data.get('width', False)
    }

    if not ret["height"]:
        ret["height"] = config['canvas-default-height']

    if not ret["width"]:
        ret["width"] = config['canvas-default-width']

    row.update(ret)


def _load_json_from_file(id, local_path):
    filepath = os.path.join(local_path, id, id + ".json")
    try:
        with open(filepath, 'r') as input_source:
            source = input_source.read()
        input_source.close()
    except FileNotFoundError:
        return {}

    return json.loads(source)


def _load_json_from_s3(s3_bucket, s3_path, id):
    s3_path = os.path.join(s3_path, id + ".json")
    print(s3_path)
    return read_s3_json(s3_bucket, s3_path)


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
