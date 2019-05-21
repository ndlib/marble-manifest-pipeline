# index_manifest.py
""" Insert Manifest information into search tool to make manifest discoverable """

import json
from get_manifest_info import append_manifest_info
from get_existing_index_record import get_existing_index_record
from create_new_index_record import create_new_index_record
from modify_existing_index_record import modify_existing_index_record
from write_index_file import write_index_file
from urllib import error
import boto3
import os
from botocore.exceptions import ClientError


def index_manifest(manifest_id, config):
    """ Augment or Insert Index record for manifest (id is either URL or unique identifier) """
    manifest_location = config['process-bucket-write-basepath'] \
        + "/" + manifest_id + "/manifest/index.json"
    try:
        manifest_json = json.loads(_read_s3_file(config['process-bucket'], manifest_location))
        manifest_info = append_manifest_info(manifest_id, manifest_json)
        indexid = manifest_info['id']
        # print(manifest_info)
        try:
            index_record = get_existing_index_record(indexid)
            index_record = modify_existing_index_record(index_record, manifest_info)
        except error.HTTPError:
            index_record = create_new_index_record(manifest_info)
        filename = manifest_info['id'] + '.xml'
        write_index_file(config['local-dir'], filename, index_record)
        remote_index_dir = config['process-bucket-index-basepath'] + "/process/"
        _copy_local_to_s3(config['local-dir'], remote_index_dir, config['process-bucket'])
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchBucket':
            print('Manifest not found for ', manifest_id, ' - returning empty manifest')
            manifest_info = append_manifest_info(manifest_id, {})
        else:
            raise
    return manifest_info


def _copy_local_to_s3(local_directory, remote_directory, bucket):
    """ Copies from local storage to S3.  Event is unique id, args contains s3 bucket """
    client = boto3.client('s3')
    for root, dirs, files in os.walk(local_directory):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, local_directory)
            s3_path = os.path.join(remote_directory, relative_path)
            try:
                client.upload_file(local_path, bucket, s3_path)
                os.remove(local_path)
            except Exception as e:
                # if ANY file fails, add err msg, and move onto next event
                print(e)
                # _add_process_error(e, event)
                return


def _read_s3_file(bucket, key):
    content_object = boto3.resource('s3').Object(bucket, key)
    return content_object.get()['Body'].read().decode('utf-8')

# python -c "import index_manifest; index_manifest.index_manifest('epistemological-letters')"
