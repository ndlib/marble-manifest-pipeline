import boto3
import json
import os

default_config = {
    "process-bucket-read-basepath": "process",
    "process-bucket-csv-basepath": "csv",
    "process-bucket-ead-resource-mappings-file": "ead_to_resource_dictionary.json",
    "rbsc-image-buckets": {
        "marble-manifest-prod-processbucket-13bond538rnnb": ["digital/bookreader", "collections/ead_xml/images"]
    },
    "canvas-default-height": 2000,
    "canvas-default-width": 2000,
    "image-data-file": "image-data.json",
    "noreply-email-addr": "noreply@nd.edu",
    "local-path": "please set me",
    "csv-field-names": ["sourceSystem", "repository", "collectionId", "parentId",
                        "myId", "level", "title", "creator", "dateCreated", "uniqueIdentifier",
                        "dimensions", "languages", "subjects", "usage", "license", "linkToSource",
                        "access", "format", "dedication", "description", "modifiedDate", "thumbnail",
                        "filePath", "fileId", "sequence", "collectionInformation", "fileInfo",
                        "classification", "workType", "medium", "artists", "digitalAssets",
                        "width", "height", "etag", "md5Checksum", "children"
                        ],
  "museum-required-fields": {
      "Title": "title",
      "Creator": "creator",
      "Date created": "dateCreated",
      "Work Type": "workType",
      "Medium": "medium",
      "Unique identifier": "uniqueIdentifier",
      "Repository": "repository",
      "Subject": "subjects",
      "Usage": "usage",
      "Access": "access",
      "Dimensions": "dimensions",
      "Dedication": "dedication",
      "Thumbnail": "digitalAssets"
    },
    "seconds-to-allow-for-processing": 780,
    "hours-threshold-for-incremental-harvest": 72,
    "archive-space-server-base-url": "https://archivesspace.library.nd.edu/oai",
}

# currently only used as reference here could be used for validation in the future
ssm_only_keys = [
    "process-bucket",
    "manifest-server-bucket",
    "image-server-buckcet",
    "image-server-base-url",
    "manifest-server-base-url"
]


def get_pipeline_config(event):
    if 'local' in event and event['local']:
        config = load_config_local(event['local-path'])
    else:
        config = load_config_ssm(event['ssm_key_base'])

    config.update(event)
    return config


def load_config_local(local_path):
    with open(local_path + "default_config.json", 'r') as input_source:
        source = json.loads(input_source.read())
    input_source.close()
    source['local'] = True
    return source


def load_config_ssm(ssm_key_base):
    config = default_config

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
    config['local'] = False

    return config


# python -c 'from pipeline_config import *; test()'
def test():
    event = {}
    event['local'] = True
    event['local-path'] = '/Users/jhartzle/Workspace/mellon-manifest-pipeline/process_manifest/../example/'
    get_pipeline_config(event)
