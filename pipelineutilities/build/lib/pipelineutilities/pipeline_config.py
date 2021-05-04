# import _set_pipelineutilites_path  # noqa
import boto3
from s3_helpers import read_s3_json, write_s3_json
import os

default_config = {
    # "process-bucket": "marble-manifest-prod-processbucket-13bond538rnnb",
    # "image-server-bucket": "marble-data-broker-publicbucket-1kvqtwnvkhra2",
    # "manifest-server-bucket": "marble-manifest-prod-manifestbucket-lpnnaj4jaxl5",
    "museum-google-credentials": "",
    "museum-google-drive-id": "",
    "museum-server-username": "",
    "museum-server-password": "",
    "rbsc-image-bucket": "rbsc-test-files",
    "multimedia-bucket": "mlk-multimedia-333680067100",
    "museum-server-base-url": "http://notredame.dom5182.com:8080",
    "process-bucket-read-basepath": "process",
    "process-bucket-csv-basepath": "csv",
    "process-bucket-data-basepath": "json",
    "process-bucket-ead-resource-mappings-file": "ead_to_resource_dictionary.json",
    "canvas-default-height": 2000,
    "canvas-default-width": 2000,
    "image-data-file": "image_data.json",
    "noreply-email-addr": "noreply@nd.edu",
    "google_keys_ssm_base": "/all/marble/google",
    "museum_keys_ssm_base": "/all/marble/museum",
    "curate_keys_ssm_base": "/all/marble/curate/prod",
    "local-path": "please set me",
    "required-fields-by-source-system": {
        "EmbARK": {
            "notify": ["hbertold@nd.edu"],
            "required-fields": {
                "Title": "title",
                "Creators": "creators",
                "Created date": "createdDate",
                "Work Type": "workType",
                "Medium": "medium",
                "Unique identifier": "uniqueIdentifier",
                "Repository": "repository",
                "Subject": "subjects",
                "Copyright Status": "copyrightStatus",
                "Access": "access",
                "Dimensions": "dimensions",
                "Dedication": "dedication"
            }
        }
    },
    "seconds-to-allow-for-processing": 600,
    "hours-threshold-for-incremental-harvest": 72,
    "archive-space-server-base-url": "https://archivesspace.library.nd.edu/oai",
    "pipeline-control-folder": "pipeline_control",
    "source-systems-requiring-metadata-expire-time": [],
    "source-systems-requiring-special-file-processing": ["Curate", "EmbARK"],
    "file-extensions-to-protect-from-changing-to-tif": [".pdf"],  # all other files will be assumed to be image files, and will be changed to .tif
    "unwanted-file-extensions-from-curate": [".jpg", ".jpeg", ".xml"],  # Curate images should be saved as both .tif and .jpg.  Skip .jpg.  Skip .xml too
}

local_ssm = {
    "process-bucket": "marble-manifest-prod-processbucket-13bond538rnnb",
    "manifest-server-bucket": "marble-manifest-prod-manifestbucket-lpnnaj4jaxl5",
    "image-server-bucket": "marble-data-broker-publicbucket-1kvqtwnvkhra2",
    "image-server-base-url": "https://image-iiif.library.nd.edu/iiif/2",
    "manifest-server-base-url": "https://presentation-iiif.library.nd.edu"
}

# currently only used as reference here could be used for validation in the future
ssm_only_keys = [
    "process-bucket",
    "manifest-server-bucket",
    "image-server-bucket",
    "image-server-base-url",
    "manifest-server-base-url"
]


def test_required_fields(event):
    for key in ['config-file', 'process-bucket']:
        if key not in event:
            raise Exception(key + " required to be in the event dictionary for pipeline config")


def setup_pipeline_config(event):
    if event.get('local', False):
        config = load_config_local()
    else:
        if 'ssm_key_base' not in event and 'SSM_KEY_BASE' in os.environ:
            event['ssm_key_base'] = os.environ['SSM_KEY_BASE']
        if "ssm_key_base" not in event:
            raise Exception("ssm_key_base required to be in the event dictionary to setup a pipeline config")
        config = default_config.copy()
        config.update(load_config_ssm(event['ssm_key_base']))
        config.update(_fix_config_url_keys(config))

        config['local'] = False  # ensure it is set and false

    # merge the current event
    config.update(event)
    return config


def _fix_config_url_keys(config):
    if config.get('image-server-base-url'):
        config['image-server-base-url'] = "https://" + config['image-server-base-url'] + '/iiif/2'

    if config.get('manifest-server-base-url'):
        config['manifest-server-base-url'] = "https://" + config['manifest-server-base-url']

    return config


def load_pipeline_config(event):
    if event.get('local', False):
        config = load_config_local()
    else:
        test_required_fields(event)
        s3Bucket = event['process-bucket']
        s3Path = "pipeline_runs/" + event['config-file']
        config = read_s3_json(s3Bucket, s3Path)

    # merge the current event
    config.update(event)
    return config


def cache_pipeline_config(config, event):
    if event.get('local', False):
        return

    test_required_fields(event)
    s3Path = "pipeline_runs/" + event['config-file']
    s3Bucket = event['process-bucket']
    write_s3_json(s3Bucket, s3Path, config)


def load_config_local():
    config = default_config.copy()
    config.update(local_ssm)
    return config


def load_config_ssm(ssm_key_base):
    config = {}

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

    return config


# python -c 'from pipeline_config import *; test()'
def test():
    event = {}
    event['local'] = True
    event['local-path'] = '/Users/jhartzle/Workspace/mellon-manifest-pipeline/process_manifest/../example/'
    setup_pipeline_config(event)
