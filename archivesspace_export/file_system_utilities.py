# file_system_utilities.py
""" this module contains simple, often-used file system utilities """

import os
import errno


def create_directory(directory):
    """ create directory if it does not exist """
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError as e:  # this allows for running multiple concurrent instances
        if e.errno != errno.EEXIST:
            raise


def delete_file(folder_name, file_name):
    """ Delete temparary intermediate file """
    full_path_file_name = get_full_path_file_name(folder_name, file_name)
    try:
        os.remove(full_path_file_name)
    except FileNotFoundError:
        pass


def get_full_path_file_name(folder_name, file_name):
    """ Build full path to file given folder and file name """
    full_path_file_name = ''
    if folder_name > '':
        full_path_file_name = folder_name  # + '/'
        if not folder_name.endswith("/"):
            full_path_file_name += "/"
    full_path_file_name += file_name
    return full_path_file_name


def copy_file_from_local_to_s3(bucket, s3_location, local_directory, file_name):
    """ Need to define something like:
        import boto3
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(config['process-bucket'])
        """
    results = False
    full_local_path = get_full_path_file_name(local_directory, file_name)
    try:
        bucket.upload_file(full_local_path, s3_location)
        results = True
    except Exception:
        results = False
    return results


def copy_file_from_s3_to_local(bucket, s3_location, local_directory, file_name):
    """ Need to define something like:
        import boto3
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(config['process-bucket'])
        """
    results = False
    full_local_path = get_full_path_file_name(local_directory, file_name)
    create_directory(local_directory)
    try:
        bucket.download_file(s3_location, full_local_path)
        results = True
    except Exception:
        results = False
    return results
