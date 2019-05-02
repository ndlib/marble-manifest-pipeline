import os
import boto3


def run(event, context):
    event['config'] = get_config()
    print(event)
    return event


# retrieve configuration from parameter store
def get_config():
    config = {
        "process-bucket-read-basepath": 'process',
        "process-bucket-write-basepath": 'finished',
        "process-bucket-index-basepath": 'index',
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
    config['noreply-email-addr'] = os.environ['NO_REPLY_EMAIL']
    config['troubleshoot-email-addr'] = os.environ['TROUBLESHOOTING_EMAIL']

    return config


run({}, None)
