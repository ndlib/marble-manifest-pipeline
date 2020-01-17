import os
import json
import sentry_sdk
import manifest_utils as mu
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

sentry_sdk.init(
    dsn=os.environ['SENTRY_DSN'],
    integrations=[AwsLambdaIntegration()]
)


def run(event, context):
    event.update(get_config())
    event['event_id'] = event.get('id')
    event['ecs-args'] = [json.dumps(event)]
    event['metadata-source-type'] = determine_source_type(event)
    return event


def determine_source_type(event):
    prefix = os.path.join(event["process-bucket-read-basepath"], event["id"]) + "/"
    result = mu.s3_list_obj_by_path(event["process-bucket"], prefix)
    keys = []

    for o in result.get('Contents'):
        id = o.get('Key').replace(prefix, "")
        keys.append(id)

    if (event["main-csv"] in keys and event['items-csv'] in keys):
        return "csv"
    elif (event['descriptive-mets-file'] in keys and event['structural-mets-file'] in keys):
        return 'mets'

    raise Exception('unable to determine metadata source type from {}'.format(keys))


# retrieve configuration from parameter store
def get_config():
    config = {
        "process-bucket-read-basepath": 'process',
        "process-bucket-write-basepath": 'finished',
        "image-server-bucket-basepath": '',
        "manifest-server-bucket-basepath": '',
        "items-csv": 'items.csv',
        "main-csv": 'main.csv',
        "descriptive-mets-file": "descriptive_metadata_mets.xml",
        "structural-mets-file": "structural_metadata_mets.xml",
        "canvas-default-height": 2000,
        "canvas-default-width": 2000,
        "image-data-file": "image_data.json",
        "schema-file": "schema.json"
    }

    path = os.environ['SSM_KEY_BASE'] + '/'
    for ps in mu.ssm_get_params_by_path(path):
        value = ps['Value']
        # change /all/stacks/mellon-manifest-pipeline/<key> to <key>
        key = ps['Name'].replace(path, '')
        # add the key/value pair
        config[key] = value

    config['image-server-base-url'] = "https://" + config['image-server-base-url'] + '/iiif/2'
    config['manifest-server-base-url'] = "https://" + config['manifest-server-base-url']
    config['noreply-email-addr'] = os.environ.get('NO_REPLY_EMAIL', '')
    config['troubleshoot-email-addr'] = os.environ.get('TROUBLESHOOTING_EMAIL', '')

    return config


# python -c 'from handler import *; test()'
def test(id):
    if (not os.environ['SSM_KEY_BASE']):
        print("you must set an SSM_KEY_BASE for parameter store in the environment")
        return

    if (not id):
        print("you must pass an id to test")

    data = {"id": id}
    print(run(data, {}))
