import os
import json
import boto3
from iiifCollection import iiifCollection


def run(event, context):
    id = event.get('id')

    s3_bucket = event['process-bucket']
    s3_schema_read_path = os.path.join(event['process-bucket-read-basepath'], id, event["schema-file"])
    s3_manifest_path = os.path.join(event['process-bucket-write-basepath'], id, 'manifest/index.json')
    s3_schema_write_path = os.path.join(event['process-bucket-write-basepath'], id, event["schema-file"])

    s3_mets_path = os.path.join(event['process-bucket-read-basepath'], id, event['descriptive-mets-file'])
    s3_mets_write_path = os.path.join(event['process-bucket-write-basepath'], id, "mets.xml")

    schema_json = json.loads(read_s3_file_content(s3_bucket, s3_schema_read_path))

    # get manifest object
    manifest = iiifCollection(event, schema_json)
    # write to s3
    write_s3_json(s3_bucket, s3_manifest_path, manifest.manifest())
    write_s3_json(s3_bucket, s3_schema_write_path, schema_json)

    # copy the mets file if it is a mets process
    if event['metadata-source-type'] == 'mets':
        copy_s3(s3_bucket, s3_mets_write_path, s3_bucket, s3_mets_path)

    return event


def read_s3_file_content(s3_bucket, s3_path):
    content_object = boto3.resource('s3').Object(s3_bucket, s3_path)
    return content_object.get()['Body'].read().decode('utf-8')


def write_s3_json(s3_bucket, s3_path, json_hash):
    s3 = boto3.resource('s3')
    s3.Object(s3_bucket, s3_path).put(Body=json.dumps(json_hash), ContentType='text/json')


def copy_s3(to_bucket, to_path, from_bucket, from_path):
    s3 = boto3.resource('s3')

    to_bucket = s3.Bucket(to_bucket)
    from_source = {
        'Bucket': from_bucket,
        'Key': from_path
    }
    to_bucket.copy(from_source, to_path)


# python -c 'from handler import *; test()'
def test():
    with open("../example/item-one-image/config.json", 'r') as input_source:
        config = json.load(input_source)
    input_source.close()

    with open("../example/item-one-image/schema.json", 'r') as input_source:
        schema = json.load(input_source)
    input_source.close()

    m = iiifCollection(config, schema)
    print(m.manifest())

#    print(iiifCollection(config, data, image_data).manifest())
