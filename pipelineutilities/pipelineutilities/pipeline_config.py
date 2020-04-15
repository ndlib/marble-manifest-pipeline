import boto3
import json
import datetime

default_config = {
    # "process-bucket": "marble-manifest-prod-processbucket-13bond538rnnb",
    # "image-server-bucket": "marble-data-broker-publicbucket-1kvqtwnvkhra2",
    # "manifest-server-bucket": "marble-manifest-prod-manifestbucket-lpnnaj4jaxl5",
    "museum-google-credentials": "",
    "museum-google-drive-id": "",
    "museum-server-username": "",
    "museum-server-password": "",
    "rbsc-image-bucket": "rbsc-test-files",
    "museum-server-base-url": "http://notredame.dom5182.com:8080",
    "process-bucket-read-basepath": "process",
    "process-bucket-csv-basepath": "csv",
    "process-bucket-ead-resource-mappings-file": "ead_to_resource_dictionary.json",
    "canvas-default-height": 2000,
    "canvas-default-width": 2000,
    "image-data-file": "image_data.json",
    "noreply-email-addr": "noreply@nd.edu",
    "google_keys_ssm_base": "/all/marble/google",
    "museum_keys_ssm_base": "/all/marble/museum",
    "curate_keys_ssm_base": "/all/marble/curate/prod",
    "local-path": "please set me",
    "csv-field-names": [
        "id",
        "sourceSystem",
        "repository",
        "collectionId",
        "parentId",
        "level",
        "title",
        "dateCreated",
        "uniqueIdentifier",
        "dimensions",
        "languages",
        "subjects",
        "copyrightStatus",
        "copyrightStatement",
        "linkToSource",
        "access",
        "format",
        "dedication",
        "description",
        "modifiedDate",
        "thumbnail",
        "filePath",
        "sequence",
        "collectionInformation",
        "fileId",
        "mimeType",
        "workType",
        "medium",
        "creators",
        # "digitalAssets",
        "width",
        "height",
        "md5Checksum",
        "creationPlace"
    ],
    "museum-required-fields": {
        "Title": "title",
        "Creators": "creators",
        "Date created": "dateCreated",
        "Work Type": "workType",
        "Medium": "medium",
        "Unique identifier": "uniqueIdentifier",
        "Repository": "repository",
        "Subject": "subjects",
        "Copyright Status": "copyrightStatus",
        "Access": "access",
        "Dimensions": "dimensions",
        "Dedication": "dedication",
        "Thumbnail": "digitalAssets"
    },
    "seconds-to-allow-for-processing": 600,
    "hours-threshold-for-incremental-harvest": 72,
    "archive-space-server-base-url": "https://archivesspace.library.nd.edu/oai"
}

# currently only used as reference here could be used for validation in the future
ssm_only_keys = [
    "process-bucket",
    "manifest-server-bucket",
    "image-server-bucket",
    "image-server-base-url",
    "manifest-server-base-url"
]


def get_pipeline_config(event):
    if 'local' in event and event['local']:
        config = load_config_local(event['local-path'])
    else:
        config = load_config_ssm(event['ssm_key_base'], default_config)

    config.update(event)
    return config


def generate_config_filename():
    return str(datetime.datetime.now()).replace(" ", "-") + ".json"


def load_cached_config(event):
    s3Path = "pipeline_runs/" + event['config-file']
    s3Bucket = event['process-bucket']

    try:
        content_object = boto3.resource('s3').Object(s3Bucket, s3Path)
        source = content_object.get()['Body'].read().decode('utf-8')
        return json.loads(source)
    except boto3.resource('s3').meta.client.exceptions.NoSuchKey:
        return {}


def cache_config(config, event):
    s3Path = "pipeline_runs/" + config['config-file']
    s3Bucket = config['process-bucket']

    s3 = boto3.resource('s3')
    s3.Object(s3Bucket, s3Path).put(Body=json.dumps(config), ContentType='text/json')


def load_config_local(local_path):
    with open(local_path + "default_config.json", 'r') as input_source:
        source = json.loads(input_source.read())
    input_source.close()
    source['local'] = True
    return source


def load_config_ssm(ssm_key_base, default_config):
    config = default_config.copy()

    # read the keys we want out of ssm
    client = boto3.client('ssm')
    paginator = client.get_paginator('get_parameters_by_path')
    path = ssm_key_base + '/'
    page_iterator = paginator.paginate(
        Path=path,
        Recursive=True,
        WithDecryption=True,)

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
    config['local'] = False

    return config


# python -c 'from pipeline_config import *; test()'
def test():
    event = {}
    event['local'] = True
    event['local-path'] = '/Users/jhartzle/Workspace/mellon-manifest-pipeline/process_manifest/../example/'
    get_pipeline_config(event)
