import os
import json
import boto3
from mapcollection import mapManifestCollection


def run(event, context):
    id = event.get('id')
    config = event.get('config')
    s3Bucket = config['process-bucket']
    s3EventPath = os.path.join(config['process-bucket-read-basepath'], id, config["event-file"])
    s3SchemaPath = os.path.join(config['process-bucket-write-basepath'], id, 'schema/index.json')
    s3 = boto3.resource('s3')
    content_object = boto3.resource('s3').Object(s3Bucket, s3EventPath)
    file_content = content_object.get()['Body'].read()
    readfile = json.loads(file_content).get('data')
    if readfile['type'].exists():
        type = readfile['type']
    else:
        type = 'manifest'
    if type.lower() == 'collection':
        mapManifestCollection(readfile, 'CreativeWorkSeries', s3EventPath, s3Bucket)
    elif type.lower() == 'manifest':
        mapManifestCollection(readfile, 'CreativeWork', s3EventPath, s3Bucket)
    else:
        print("Unknown Manifest")
        return {
            'statusCode': 415
        }
    readfile.update({"seeAlso": s3SchemaPath})
    s3.Object(s3Bucket, s3SchemaPath).put(Body=json.dumps(readfile), ContentType='text/json')
    return {
        'statusCode': 200
    }


def test():

    with open("../example/example-input.json", 'r') as input_source:
        data = json.load(input_source)
    input_source.close()
    data = {
      "id": "example",
      "config": data["config"],
      "manifestData": data
    }
    print(run(data, {}))
