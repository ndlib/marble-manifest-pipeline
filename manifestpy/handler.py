import os
import json
import boto3
from mapmain import mapMainManifest


def run(event, context):
    id = event.get('id')
    config = event.get('config')
    s3Bucket = config['process-bucket']
    s3EventPath = os.path.join(config['process-bucket-read-basepath'], id, config["event-file"])
    s3SchemaPath = os.path.join(config['process-bucket-write-basepath'], id, 'schema/index.json')
    s3ManifestPath = os.path.join(config['process-bucket-write-basepath'], id, 'manifest/index.json')
    s3 = boto3.resource('s3')
    content_object = boto3.resource('s3').Object(s3Bucket, s3EventPath)
    file_content = content_object.get()['Body'].read()
    readfile = json.loads(file_content).get('data')

    try:
        readfile['type']
    except KeyError:
        type = 'manifest'
    else:
        type = readfile['type']
    if type.lower() == 'collection':
        mainOut = mapMainManifest(readfile, 'CreativeWorkSeries')
    elif type.lower() == 'manifest':
        mainOut = mapMainManifest(readfile, 'CreativeWork')
    else:
        print("Unknown Manifest")
        return {
            'statusCode': 415
        }

    manifest_object = boto3.resource('s3').Object(s3Bucket, s3EventPath)
    manifest_content = manifest_object.get()['Body'].read()
    writefile = json.loads(manifest_content).get('data')
    seeAlso = {"seeAlso": s3SchemaPath}
    writefile.update(seeAlso)
    s3.Object(s3Bucket, s3ManifestPath).put(Body=json.dumps(writefile), ContentType='text/json')
    s3.Object(s3Bucket, s3SchemaPath).put(Body=json.dumps(mainOut), ContentType='text/json')

    return event


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
