import os
import json
import boto3
from iiifCollection import iiifCollection


def run(event, context):
    id = event.get('id')
    s3_bucket = event['process-bucket']
    s3_event_path = os.path.join(event['process-bucket-read-basepath'], id, event["event-file"])
    s3_image_data_path = os.path.join(event['process-bucket-read-basepath'], id, event["image-data-file"])
    s3_manifest_path = os.path.join(event['process-bucket-write-basepath'], id, 'manifest/index.json')

    # for testing see test() below.
    # This allows this to be run locally without having a file in s3
    manifestData = json.loads(read_s3_file_content(s3_bucket, s3_event_path))
    imageData = json.loads(read_s3_file_content(s3_bucket, s3_image_data_path))

    # get manifest object
    manifest = iiifCollection(event, manifestData, imageData)
    # write to s3
    write_s3_json(s3_bucket, s3_manifest_path, manifest.manifest())

    return event


def read_s3_file_content(s3_bucket, s3_path):
    content_object = boto3.resource('s3').Object(s3_bucket, s3_path)
    return content_object.get()['Body'].read().decode('utf-8')


def write_s3_json(s3_bucket, s3_path, json_hash):
    s3 = boto3.resource('s3')
    s3.Object(s3_bucket, s3_path).put(Body=json.dumps(json_hash), ContentType='text/json')


# python -c 'from handler import *; test()'
def test():
    with open("../example/collection-small/config.json", 'r') as input_source:
        config = json.load(input_source)
    input_source.close()

    with open("../example/item-one-image/schema.json", 'r') as input_source:
        schema = json.load(input_source)
    input_source.close()

    m = iiifCollection(config, schema)
    print(m.manifest())

#    print(iiifCollection(config, data, image_data).manifest())
