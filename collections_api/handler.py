"""This module used in a lambda and api gateway to deal with collections."""

import _set_path  # noqa
import os
import json
from datetime import datetime, date
import boto3
import re
from pipeline_config import load_pipeline_config
import sentry_sdk as sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
from s3_helpers import read_s3_json

if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(
        dsn=os.environ['SENTRY_DSN'],
        integrations=[AwsLambdaIntegration()]
    )


def run(event, context):
    config = load_pipeline_config(event)
    collections_list = get_collections_list(config, event.get("source", ""))
    if not len(collections_list):
        return error('404')
    return success(collections_list)


def run_id(event, context):
    config = load_pipeline_config(event)
    item_json = get_id(config, event.get('id', ""))
    if not item_json:
        return error('404')
    return success(item_json)


def get_id(config: dict, id: str) -> dict:
    """ Return content for an individual id."""
    item_json = {}
    key = os.path.join(config["process-bucket-data-basepath"], id + '.json')
    print(config['process-bucket'])
    print("key = ", key)
    if id:
        item_json = read_s3_json(config['process-bucket'], key)
    return item_json


def get_collections_list(config, source: str = "") -> list:
    """ Get a listing of collections by source.  If no source is specified, return all."""
    collections_list = []
    folder = config["process-bucket-data-basepath"]
    patterns = {
        "aleph": "(^" + folder + r"/[0-9]{9}\.json$)",
        "museum": "(^" + folder + r"/[A-Z]*[0-9]{4}.[.0-9]*[.a-z]*\.json$)",
        "rbsc": "(^" + folder + r"/[A-Z]*[0-9]*_EAD\.json$)",
        "curate": "(^" + folder + r"/[a-z0-9]{11}\.json$)"
    }
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    if source:
        regex = patterns.get(source, "^source was specified but no pattern found so do not return results")
    else:
        regex = ".*"  # if no source was indicated, return everything
    for result in paginator.paginate(Bucket=config["process-bucket"], Prefix=config["process-bucket-data-basepath"]):
        for file_info in result.get('Contents', []):
            key = file_info.get('Key', '')
            if re.search(regex, key):
                collections_list.append(re.sub('^' + folder + '/', '', re.sub('.json$', '', key)))
    collections_list.sort()
    return collections_list


def error(code):
    return {
        "statusCode": code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
        },
    }


def success(data):
    return {
        "statusCode": 200,
        "body": json.dumps(data, default=json_serial),
        "headers": {
            "Access-Control-Allow-Origin": "*",
        },
    }


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


# export SSM_KEY_BASE=/all/new-csv
# aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
# python -c 'from handler import *; test()'
def test():
    event = {}
    event['local'] = True
    event['source'] = 'curate'
    print(run(event, {}))

    event['id'] = '1934.007.001'
    print("id content = ", run_id(event, {}))
