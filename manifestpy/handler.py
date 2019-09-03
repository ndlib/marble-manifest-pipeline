import os
import json
import boto3
from mapcollection import mapManifestCollection


def run(event, context):
    id = event.get('id')
    config = event.get('config')
    s3Bucket = config['process-bucket']
    s3SchemaPath = os.path.join(config['process-bucket-write-basepath'], id, 'schema/index.json')
    s3 = boto3.client('s3')
    bucket_name = event['config']['manifest-server-bucket']
    file_key = event['config']['event-file']
    rfile = s3.get_object(Bucket=bucket_name, Key=file_key)
    file_folder = file_key.split('.', 1)[0]
    also_location = bucket_name+'.s3.amazonaws.com/finished/'+file_folder
    readfile = json.load(rfile['Body'])
    type = readfile['type']
    if type.lower() == 'collection':
        mapManifestCollection(readfile, 'CreativeWorkSeries', file_folder, bucket_name)
    elif type.lower() == 'manifest':
        mapManifestCollection(readfile, 'CreativeWork', file_folder, bucket_name)
    else:
        print("Unknown Manifest")
        return {
            'statusCode': 415
        }
    readfile.update({"seeAlso": also_location})
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
