import boto3
import sys
import json
from pathlib import Path
import os
where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am)
sys.path.append(where_i_am + "/dependencies")

from dependencies.pipelineutilities.csv_collection import load_csv_data
from dependencies.pipelineutilities.pipeline_config import get_pipeline_config

import dependencies.sentry_sdk
from dependencies.sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
# sentry_sdk.init(
#    dsn=os.environ['SENTRY_DSN'],
#    integrations=[AwsLambdaIntegration()]
# )


def run(event, context):
    if 'ssm_key_base' not in event:
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']

    config = get_pipeline_config(event)

    ids = event.get("ids")
    for id in ids:
        parent = load_csv_data(id, config)
        # a2s = AthenaToSchema(event, parent, [])
        iiif = iiifCollection(event, parent)

        with open("./" + id + ".json", "w") as output_source:
            output_source.write(json.dumps(iiif.manifest()))

        print(json.dumps(iiif.manifest()))

    return event


def read_s3_file_content(s3Bucket, s3Path):
    content_object = boto3.resource('s3').Object(s3Bucket, s3Path)
    return content_object.get()['Body'].read().decode('utf-8')


def write_s3_json(s3Bucket, s3Path, json_hash):
    s3 = boto3.resource('s3')
    s3.Object(s3Bucket, s3Path).put(Body=json.dumps(json_hash), ContentType='text/json')


# python -c 'from handler import *; test()'
def test():
    # import pprint
    # pp = pprint.PrettyPrinter(indent=4)
    event = {}
    event['ids'] = ['parsons', '1976.057']
    event['ssm_key_base'] = '/all/new-csv'
    event['csv-data-files-bucket'] = 'marble-manifest-test-processbucket-19w6raq5mndlo'
    event['csv-data-files-basepath'] = 'archives-space-csv-files'
    event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
    run(event, {})
