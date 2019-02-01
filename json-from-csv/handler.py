import os
import boto3
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

from processCsv import processCsv

def run(event, context):
    event['config'] = get_config()

    csvSet = processCsv(event.get("id"), event.get("config"))
    if not csvSet.verifyCsvExist():
        raise Exception(csvSet.error)

    csvSet.buildJson()
    csvSet.writeEventData({ "data": csvSet.result_json })
    return event

# retrieve configuration from parameter store
def get_config():
    client = boto3.client('ssm')
    paginator = client.get_paginator('get_parameters_by_path')
    path = '/all/stacks/mellon-manifest-pipeline/'
    page_iterator = paginator.paginate(
        Path = path,
        Recursive=True,
        WithDecryption=False,
        MaxResults=4,)

    response = []
    for page in page_iterator:
        response.extend(page['Parameters'])
    
    config = {}
    for ps in response:
        value = ps['Value']
        # change height/width values from strings to numeric
        if(ps['Name'].endswith('canvas-default-height') or
            ps['Name'].endswith('canvas-default-width')):
                value = int(ps['Value'])
        # complete the partial value in parameter store with OS value
        if(ps['Name'].endswith('image-server-base-url')):
            value = ps['Value'].replace('STUB', os.environ['IMAGE_SERVER_URL'])
        # complete the partial value in parameter store with OS value
        if(ps['Name'].endswith('manifest-server-base-url')):
            value = ps['Value'] + os.environ['MANIFEST_URL']
        # change /all/stacks/mellon-manifest-pipeline/<key> to <key>
        key = ps['Name'].replace(path,'')
        # add the key/value pair
        config[key] = value

    config['process-bucket'] = os.environ['PROCESS_BUCKET']
    config['image-server-bucket'] = os.environ['IMAGE_BUCKET']
    config['manifest-server-bucket'] = os.environ['MANIFEST_BUCKET']
    config['notify-on-finished']: os.environ['NOTIFY_EMAILS']

    return config

# python -c 'from handler import *; test()'
def test():
    data = { "id": "example"}
    print(run(data, {}))
