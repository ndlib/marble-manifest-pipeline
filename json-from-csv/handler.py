import os
import boto3
import json
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from processCsv import processCsv


def run(event, context):
    id = event.get("id")
    config = get_config()

    process_bucket = config['process-bucket']
    main_key = config['process-bucket-read-basepath'] + "/" + id + "/" + config['main-csv']
    sequence_key = config['process-bucket-read-basepath'] + "/" + id + "/" + config['sequence-csv']
    event_key = config['process-bucket-read-basepath'] + "/" + id + "/" + config["event-file"]

    main_csv = readS3FileContent(process_bucket, main_key)
    sequence_csv = readS3FileContent(process_bucket, sequence_key)

    csvSet = processCsv(id, config, main_csv, sequence_csv)

    csvSet.buildJson()

    copy_default_img(id, config)
    writeS3Json(process_bucket, event_key, {"data": csvSet.result_json})

    event['config'] = config
    return event


# retrieve configuration from parameter store
def get_config():
    config = {
        "process-bucket-read-basepath": 'process',
        "process-bucket-write-basepath": 'finished',
        "image-server-bucket-basepath": '',
        "manifest-server-bucket-basepath": '',
        "sequence-csv": 'sequence.csv',
        "main-csv": 'main.csv',
        "canvas-default-height": 2000,
        "canvas-default-width": 2000,
        "event-file": "event.json"
    }

    # read the keys we want out of ssm
    client = boto3.client('ssm')
    paginator = client.get_paginator('get_parameters_by_path')
    path = os.environ['SSM_KEY_BASE'] + '/'
    page_iterator = paginator.paginate(
        Path=path,
        Recursive=True,
        WithDecryption=False,)

    response = []
    for page in page_iterator:
        response.extend(page['Parameters'])

    for ps in response:
        value = ps['Value']
        # change /all/stacks/mellon-manifest-pipeline/<key> to <key>
        key = ps['Name'].replace(path, '')
        # add the key/value pair
        config[key] = value

    config['image-server-base-url'] = "https://" + config['image-server-base-url'] + ':8182/iiif/2'
    config['manifest-server-base-url'] = "https://" + config['manifest-server-base-url']

    return config


def readS3FileContent(s3Bucket, s3Path):
    content_object = boto3.resource('s3').Object(s3Bucket, s3Path)
    return content_object.get()['Body'].read().decode('utf-8')


def writeS3Json(s3Bucket, s3Path, json_hash):
    s3 = boto3.resource('s3')
    s3.Object(s3Bucket, s3Path).put(Body=json.dumps(json_hash), ContentType='text/json')

# clones an established default image to an image named default.jpg
def copy_default_img(id, config):
    bucket = config['process-bucket']
    remote_file = config['process-bucket-read-basepath'] + "/" + id + "/images/" + config['default-img']
    default_image = config['process-bucket-read-basepath'] + "/" + id + "/images/default.jpg"
    copy_source = {
        'Bucket': bucket,
        'Key': remote_file
    }
    boto3.resource('s3').Bucket(bucket).copy(copy_source, default_image)


# python -c 'from handler import *; test()'
def test():
    data = {"id": "2018_example_001"}
    print(run(data, {}))
