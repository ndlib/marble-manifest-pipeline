# import csv
import json
import os
import boto3
# from io import StringIO


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


# def load_csv_data(id, config):
#     if config.get('local', False):
#         objects = load_id_from_file(id, config)
#     else:
#         objects = load_id_from_s3(config['process-bucket'], config['process-bucket-csv-basepath'], id)
#
#     all_image_data = load_image_data(id, config)
#
#     for object in objects:
#         _augment_row_data(object, all_image_data, config)
#
#     return Item(objects[0], objects).collection()
#
#
# def load_id_from_s3(s3Bucket, s3Path, id):
#     s3Path = s3Path + "/" + id + ".csv"
#
#     content_object = boto3.resource('s3').Object(s3Bucket, s3Path)
#     source = content_object.get()['Body'].read().decode('utf-8')
#     f = StringIO(source)
#
#     return list(csv.DictReader(f, delimiter=','))
#
#
# def load_id_from_file(id, config):
#     filepath = config['local-path'] + "/" + id + "/" + id + ".csv"
#
#     with open(filepath, 'r') as input_source:
#         source = input_source.read()
#     input_source.close()
#     f = StringIO(source)
#
#     return list(csv.DictReader(f, delimiter=','))

#
# class Item():
#
#     def __init__(self, object, all_objects):
#         self.object = object
#         self.all_objects = all_objects
#
#     def repository(self):
#         return self.get('sourceSystem', 'aleph')
#
#     def type(self):
#         return self.get('level')
#
#     def collection(self):
#         return self._find_row(self.object.get('collectionId'))
#
#     def parent(self):
#         parent = self._find_row(self.object.get('parentId'))
#         if not parent:
#             parent = self
#
#         return parent
#
#     def root(self):
#         return self.get("parentId", False) == "root"
#
#     def get(self, key, default=False):
#         return self.object.get(key, default)
#
#     def children(self):
#         children = []
#         test_id = "".join(self.get('id').lower().split(" "))
#         for row in self.all_objects:
#             if "".join(row['parentId'].lower().split(" ")) == test_id:
#                 children.append(Item(row, self.all_objects))
#
#         return children
#
#     def files(self):
#         ret = []
#
#         for child in self.children():
#             if child.type() == 'file':
#                 ret.append(child)
#             else:
#                 ret = ret + child.files()
#
#         return ret
#
#     def _find_row(self, id):
#         for this_row in self.all_objects:
#             if this_row.get('id', False) == id:
#                 return Item(this_row, self.all_objects)
#
#         return False


def _augment_row_data(row, all_image_data, config):
    _turn_strings_to_json(row)
    _fix_ids(row)
    _check_creator(row)
    _add_additional_paths(row, config)
    _add_image_dimensions(row, all_image_data, config)


# turns out that the json method is changing these to floats
# even though my initial test said they were going to fail with the type TypeError
def _fix_ids(row):
    row["id"] = str(row["id"])
    row["collectionId"] = str(row["collectionId"])
    row["parentId"] = str(row["parentId"])


def _turn_strings_to_json(row):
    for key in row.keys():
        if ("{" in row[key] and "}" in row[key] or "[" in row[key] and "]" in row[key]):
            try:
                row[key] = json.loads(row[key])
            # we are simply testing if this is valid json if it is not and fails do nothing
            # i realize this is an antipattern
            except (ValueError, TypeError):
                pass


def _check_creator(row):
    if not (row.get("creators", False) or row.get('creators')) and (row.get("level") == "collection" or row.get("level") == "manifest"):
        row["creators"] = [{"fullName": "unknown"}]


def _add_additional_paths(row, config):
    level = row.get('level')
    if level == "file":
        paths = _file_paths(row, config)
    elif level == "manifest" or level == "collection":
        paths = _manifest_paths(row, config)
    else:
        if level is None:
            level = "NoneType"
        raise ValueError("Level must be one of ('collection', 'manifest', 'file').  You passed: " + level)
    row.update(paths)


def _file_paths(row, config):
    id_no_extension = os.path.splitext(row.get('id'))[0]
    uri_path = '/' + row.get('collectionId') + '%2F' + id_no_extension
    path = '/' + row.get('collectionId') + "/" + id_no_extension

    return {
        "iiifImageUri": config['image-server-base-url'] + uri_path,
        "iiifImageFilePath": "s3://" + config['image-server-bucket'] + path,
        "iiifUri": config["manifest-server-base-url"] + path + "/canvas",
        "iiifFilePath": "s3://" + config['manifest-server-bucket'] + path + "/canvas/index.json",
        "metsUri": "",
        "metsFilePath": "",
        "schemaUri": "",
        "schemaPath": "",
    }


def _manifest_paths(row, config):
    path = "/" + row.get('collectionId')
    if row.get('collectionId') != row.get('id'):
        path = path + "/" + row.get("id")

    return {
        "iiifImageUri": "",
        "iiifImageFilePath": "",
        "iiifUri": config["manifest-server-base-url"] + path + "/manifest",
        "iiifFilePath": "s3://" + config['manifest-server-bucket'] + path + "/manifest",
        "metsUri": config["manifest-server-base-url"] + path + "/mets.xml",
        "metsFilePath": "s3://" + config['manifest-server-bucket'] + path + "/mets.xml",
        "schemaUri": config["manifest-server-base-url"] + path,
        "schemaPath": "s3://" + config['manifest-server-bucket'] + path + "/index.json",
    }


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


# python -c 'from csv_collection import *; test()'
def test():
    from pipeline_config import setup_pipeline_config
    event = {"local": False}
    event['local-path'] = '/Users/jhartzle/Workspace/mellon-manifest-pipeline/process_manifest/../example/'

    event['ssm_key_base'] = "/all/marble-manifest-deployment/prod"
    config = setup_pipeline_config(event)

    # s3 libnd
    config['local'] = False
    # for id in ['1954.030']:
    #     parent = load_csv_data(id, config)
    #     print(parent.object['creators'][0])
    # return
    # # local
    # config['local'] = True
    # for id in ['parsons', '1976.057']:
    #     parent = load_csv_data(id, config)
    #     print(parent.get('title'))
    #     print(parent.get('height'))
    #
    #     for file in parent.files():
    #         print(file.get("height"))
