import os
import json
import boto3
from iiifManifest import iiifManifest


def run(event, context):
    id = event.get('id')
    config = event.get('config')
    s3Bucket = config['process-bucket']
    s3EventPath = os.path.join(config['process-bucket-write-basepath'], id, config["event-file"])
    s3ManifestPath = os.path.join(config['process-bucket-write-basepath'], id, '/manifest/index.json')

    # for testing see test() below.
    # This allows this to be run locally without having a file in s3
    if event.get('manifestData'):
        manifestData = event.get('manifestData')
    else:
        manifestData = readS3Json(s3Bucket, s3EventPath)
        print(manifestData)

    # get manifest object
    manifest = iiifManifest(id, config, manifestData)
    # write to s3
    writeS3Json(s3Bucket, s3ManifestPath, manifest.manifest())

    return event


def readS3Json(s3Bucket, s3Path):
    content_object = boto3.resource('s3').Object(s3Bucket, s3Path)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    return json.loads(file_content).get('data')


def writeS3Json(s3Bucket, s3Path, manifest):
    s3 = boto3.resource('s3')
    s3.Object(s3Bucket, s3Path).put(Body=json.dumps(manifest), ContentType='text/json')


# python -c 'from handler import *; test()'
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
