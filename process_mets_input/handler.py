import boto3
import json
from mets_to_schema import MetsToSchema
from pathlib import Path


def run(event, context):
    id = event.get("id")

    process_bucket = event['process-bucket']
    descriptive_metadata_key = event['process-bucket-read-basepath'] + "/" + id + "/" + event['descriptive-metadata-file']
    structural_metadata_key = event['process-bucket-read-basepath'] + "/" + id + "/" + event['structural-mets-file']
    image_key = event['process-bucket-read-basepath'] + "/" + id + "/" + event["image-data-file"]
    schema_key = event['process-bucket-read-basepath'] + "/" + id + "/" + event["schema-file"]

    descriptive_metadata_xml = read_s3_file_content(process_bucket, descriptive_metadata_key)
    structural_metadata_xml = read_s3_file_content(process_bucket, structural_metadata_key)
    image_data = json.loads(read_s3_file_content(process_bucket, image_key))

    metsSet = MetsToSchema(event, descriptive_metadata_xml, structural_metadata_xml, image_data)

    write_s3_json(process_bucket, schema_key, metsSet.get_json())

    return event


def read_s3_file_content(s3Bucket, s3Path):
    content_object = boto3.resource('s3').Object(s3Bucket, s3Path)
    return content_object.get()['Body'].read().decode('utf-8')


def write_s3_json(s3Bucket, s3Path, json_hash):
    s3 = boto3.resource('s3')
    s3.Object(s3Bucket, s3Path).put(Body=json.dumps(json_hash), ContentType='text/json')


# python -c 'from handler import *; test()'
def test():
    current_path = str(Path(__file__).parent.absolute())

    with open(current_path + "/../example/item-one-image/config.json", 'r') as input_source:
        event = json.load(input_source)
    input_source.close()

    with open(current_path + "/../example/item-one-image/image-data.json", 'r') as input_source:
        image_data = json.load(input_source)
    input_source.close()

    with open(current_path + "/../example/item-one-image/descriptive_metadata_mets.xml", 'r') as input_source:
        descriptive_metadata_xml = input_source.read()
    input_source.close()

    with open(current_path + "/../example/item-one-image/structural_metadata_mets.xml", 'r') as input_source:
        structural_metadata_xml = input_source.read()
    input_source.close()

    metsSet = MetsToSchema(event, descriptive_metadata_xml, structural_metadata_xml, image_data)
    print(metsSet.get_json())
