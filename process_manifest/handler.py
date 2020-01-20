import boto3
import sys
import json
from pathlib import Path
from pipelineutilities.csv_collection import load_csv_data
from pipelineutilities.pipeline_config import get_pipeline_config

import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
# sentry_sdk.init(
#    dsn=os.environ['SENTRY_DSN'],
#    integrations=[AwsLambdaIntegration()]
# )


def run(event, context):
    ids = event.get("ids")
    config = get_pipeline_config(event)
    print(config)
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
    event['local'] = True
    event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
    run(event, {})
