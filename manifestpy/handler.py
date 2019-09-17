import os
import json
import boto3
from mapmain import mapMainManifest


def run(event, context):
    id = event.get('id')

    s3_bucket = event['process-bucket']
    s3_event_json_path = os.path.join(event['process-bucket-read-basepath'], id, event["event-file"])
    s3_schema_json_path = os.path.join(event['process-bucket-write-basepath'], id, 'schema/index.json')

    event_json = json.loads(read_s3_file_content(s3_bucket, s3_event_json_path))

    schema_dict = get_schema_dict(event_json)
    event_json = add_schema_to_event_json(event_json, event)

    # write the
    write_s3_json(s3_bucket, s3_event_json_path, event_json)
    write_s3_json(s3_bucket, s3_schema_json_path, schema_dict)

    return event


def add_schema_to_event_json(event_json, event):
    schema_url = os.path.join(event.get('manifest-server-base-url'), event.get('id'), 'schema')

    see_also = {
        "id": schema_url,
        "format": "application/ld+json",
        "profile": "http://iiif.io/community/profiles/discovery/schema"
    }

    event_json['seeAlso'] = event_json.get('seeAlso', [])
    event_json['seeAlso'].append(see_also)

    return event_json


def get_schema_dict(event_json):
    type = event_json.get('type', 'manifest').lower()

    if type == 'collection':
        return mapMainManifest(event_json, 'CreativeWorkSeries')
    elif type == 'manifest':
        return mapMainManifest(event_json, 'CreativeWork')

    raise "type, {}, not supported.".format(type)


def read_s3_file_content(s3_bucket, s3_path):
    content_object = boto3.resource('s3').Object(s3_bucket, s3_path)
    return content_object.get()['Body'].read().decode('utf-8')


def write_s3_json(s3_bucket, s3Path, manifest):
    s3 = boto3.resource('s3')
    s3.Object(s3_bucket, s3Path).put(Body=json.dumps(manifest), ContentType='text/json')


def test():
    with open("../example/item-one-image/config.json", 'r') as input_source:
        event = json.load(input_source)
    input_source.close()

    with open("../example/item-one-image/event.json", 'r') as input_source:
        event_json = json.load(input_source)
    input_source.close()

    schema_dict = get_schema_dict(event_json)
    event_json = add_schema_to_event_json(event_json, event)

    print("SCHEMA:")
    print(schema_dict)
    print("EVENT_JSON:")
    print(event_json)
