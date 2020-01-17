import boto3
import json
import os
from pathlib import Path


def get_pipeline_config(event):
    if event['local']:
        return load_config_local()
    else:
        return load_config_ssm(event['ssm_key_base'])


def load_config_local():
    current_path = str(Path(__file__).parent.absolute())

    with open(current_path + "/../example/default_config.json", 'r') as input_source:
        source = json.loads(input_source.read())
    input_source.close()
    source['local'] = True
    return source


def load_config_ssm(ssm_key_base):
    config = {
        "process-bucket-read-basepath": 'process',
        "process-bucket-write-basepath": 'finished',
        "image-server-bucket-basepath": '',
        "manifest-server-bucket-basepath": '',
        "canvas-default-height": 2000,
        "canvas-default-width": 2000,
        "image-data-file": "image-data.json",
        "schema-file": "schema.json"
    }

    # read the keys we want out of ssm
    client = boto3.client('ssm')
    paginator = client.get_paginator('get_parameters_by_path')
    path = ssm_key_base + '/'
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

    config['image-server-base-url'] = "https://" + config['image-server-base-url'] + '/iiif/2'
    config['manifest-server-base-url'] = "https://" + config['manifest-server-base-url']
    config['noreply-email-addr'] = os.environ.get('NO_REPLY_EMAIL', '')
    config['troubleshoot-email-addr'] = os.environ.get('TROUBLESHOOTING_EMAIL', '')
    config['local'] = False

    return config


# python -c 'from pipeline_config import *; test()'
def test():
    ""
