"""This module used in a lambda and api gateway to send list of directories."""

import _set_path  # noqa
import os
import json
from datetime import datetime, date
# from api_helpers import success
from search_files import list_all_directories
from pipeline_config import load_pipeline_config
from pipelineutilities.s3_helpers import read_s3_json, write_s3_json
import sentry_sdk as sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(
        dsn=os.environ['SENTRY_DSN'],
        integrations=[AwsLambdaIntegration()]
    )


def run(event, context):
    config = load_pipeline_config(event)

    directories = load_from_s3_or_cache(config, False)
    output = []
    for key, value in directories.items():
        output.append(convert_directory_to_json(value))

    cache_s3_call("./cache/directories.json", output)
    return success(directories)


def run_id(event, context):
    config = load_pipeline_config(event)

    directories = load_from_s3_or_cache(config, False)
    if (not directories.get(event['id'], False)):
        return error('404')

    output = convert_directory_to_json(directories[event['id']], True)
    cache_s3_call("./cache/%s.json" % (event['id']), output)

    return success(output)


def cache_directory_to_file_name(bucket, directory):
    return bucket + "-" + directory.replace("/", "-")


def cache_s3_call(file_name, objects):
    with open(file_name, 'w') as outfile:
        json.dump(objects, outfile, default=json_serial, sort_keys=True, indent=2)


def load_from_s3_or_cache(config, force_use_s3=False):
    file_name = "list_directories_cache"
    if config['test']:
        directory = os.path.join(os.path.dirname(__file__), '../test/fixtures/')
    elif config['local']:
        directory = './cache/'

    cache_file = "%s%s.json" % (directory, file_name)
    if (os.path.exists(cache_file) and not force_use_s3) or config.get("test", False) or config.get('local', False):
        file = open(cache_file, "r")
        return json.loads(file.read())
    else:
        objects = list_all_directories(config)
        cache_s3_call(cache_file, objects)
        return objects


def convert_directory_to_json(file, traverse=False):
    data = {}
    data['id'] = file.get('id')
    data['uri'] = 'https://presentation-iiif.library.nd.edu/directories/' + data['id']
    data['path'] = file.get('path')
    if (len(file.get('objects')) > 2):
        data['mode'] = 'multi-volume'
    else:
        data['mode'] = 'single-volume'

    count = 0
    if (traverse):
        data['objects'] = []

    for key, value in file['objects'].items():
        if traverse:
            data['objects'].append(convert_object_to_json(value))
        count += len(value['files'])

    data['numberOfFiles'] = count

    return data


def convert_object_to_json(object):
    object = {k[0].lower() + k[1:]: v for k, v in object.items()}
    object['uri'] = 'https://presentation-iiif.library.nd.edu/directories/' + object['directory_id'] + "/objects/" + object['id']

    for key, file in enumerate(object['files']):
        file = {k[0].lower() + k[1:]: v for k, v in file.items()}
        object['files'][key] = file

    return object


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


# python -c 'from handler import *; test()'
def test():
    data = {}
    # data['local'] = True
    # data['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
    # data['process-bucket-csv-basepath'] = ""
    data['ssm_key_base'] = '/all/marble-manifest-prod'
    data[''] = '/all/marble-manifest-prod'
    data['local'] = True
    data['rbsc-image-bucket'] = 'libnd-smb-rbsc'
    print(run(data, {}))

    data['id'] = 'collections-ead_xml-images-BPP_1001'
    run_id(data, {})

    data['directory_id'] = 'collections-ead_xml-images-BPP_1001'
    data['id'] = 'collections-ead_xml-images-BPP_1001-BPP_1001-001'
    print(run_item_id(data, {}))
