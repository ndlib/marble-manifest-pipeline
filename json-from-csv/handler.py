import os
import boto3
import json
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from processCsv import processCsv


def run(event, context):
    id = event.get("id")
    config = event.get("config")

    process_bucket = config['process-bucket']
    main_key = config['process-bucket-read-basepath'] + "/" + id + "/" + config['main-csv']
    items_key = config['process-bucket-read-basepath'] + "/" + id + "/" + config['items-csv']
    image_key = config['process-bucket-read-basepath'] + "/" + id + "/" + config["image-data-file"]
    event_key = config['process-bucket-read-basepath'] + "/" + id + "/" + config["event-file"]

    main_csv = read_s3_file_content(process_bucket, main_key)
    items_csv = read_s3_file_content(process_bucket, items_key)
    image_data = json.loads(read_s3_file_content(process_bucket, image_key))

    csvSet = processCsv(id, config, main_csv, items_csv, image_data)

    csvSet.buildJson()

    copy_default_img(id, config)
    write_s3_json(process_bucket, event_key, {"data": csvSet.result_json})

    event['config'] = config
    return event


def read_s3_file_content(s3Bucket, s3Path):
    content_object = boto3.resource('s3').Object(s3Bucket, s3Path)
    return content_object.get()['Body'].read().decode('utf-8')


def write_s3_json(s3Bucket, s3Path, json_hash):
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


event = {
    "id": "bitter",
    "config": {
        "process-bucket": 'manifest-devred-processbucket-k430pz5alyks',
        "process-bucket-read-basepath": 'process',
        "process-bucket-write-basepath": 'finished',
        "process-bucket-index-basepath": 'index',
        "image-server-bucket-basepath": '',
        "manifest-server-bucket-basepath": '',
        "items-csv": 'items.csv',
        "main-csv": 'main.csv',
        "canvas-default-height": 2000,
        "canvas-default-width": 2000,
        "image-data-file": "image_data.json",
        "event-file": "event.json"
    }
}

run(event, None)
