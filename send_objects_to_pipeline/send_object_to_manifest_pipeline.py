# send_object_to_manifest_pipeline.py
""" This module processes an individual object to copy it to the manifest pipeline """

import time
import boto3
from botocore.exceptions import ClientError
import locale
import json
import os
from google_utilities import download_google_file
from file_system_utilities import delete_file  # , get_full_path_file_name  # noqa: E402

seconds_to_allow_for_processing = 14 * 60
start_time = time.time()


def process_objects(google_connection, config, objects_needing_processed):
    """ Loop through all objects needing process, calling a routine to process each one
        that has not already been successfully processed and for which we can find all images """
    objects_still_needing_processed = []  # initialize this here in case we need to resume processing later
    for metadata_info in objects_needing_processed:
        object_already_processed = False
        if 'objectSuccessfullyProcessed' in metadata_info:
            object_already_processed = metadata_info['objectSuccessfullyProcessed']
        if not object_already_processed and metadata_info['allImagesFound']:
            success = _process_individual_object(google_connection, config, metadata_info)
            metadata_info['objectSuccessfullyProcessed'] = success
            _start_next_step(metadata_info['objectId'])

        if int(time.time() - start_time) > (seconds_to_allow_for_processing):
            print("Time expired. Saving objects still needing processed and exiting")
            objects_still_needing_processed = _return_objects_still_needing_processed(objects_needing_processed)
            break
    return objects_still_needing_processed


def _start_next_step(id):
    print("starting")
    statemachine_arn = os.environ['STATE_MACHINE']
    client = boto3.client('stepfunctions')
    response = client.start_execution(
        stateMachineArn=statemachine_arn,
        input=json.dumps({"id": id})
    )
    return response


def _return_objects_still_needing_processed(objects_needing_processed):
    objects_still_needing_processed = []
    for object in objects_needing_processed:
        object_successfully_processed = False
        if 'objectSuccessfullyProcessed' in object:
            object_successfully_processed = object['objectSuccessfullyProcessed']
        if not object_successfully_processed:
            objects_still_needing_processed.append(object)
    return objects_still_needing_processed


def _process_individual_object(google_connection, config, metadata_info):
    """ Copy individual object to S3 to feed manifest pipeline """
    success = False
    object_id = metadata_info['objectId']
    bucket_name = config['process-bucket']
    object_folder_name, image_folder_name = _create_folders_in_s3(bucket_name, config, object_id)
    all_images_uploaded = _copy_images_from_google_drive_to_s3(google_connection, metadata_info, bucket_name, image_folder_name)  # noqa: E501
    if all_images_uploaded:
        _copy_metadata_files_from_google_drive_to_s3(google_connection, bucket_name,
                                                     metadata_info, object_id, object_folder_name)
        success = metadata_info['allImagesUploaded'] \
            and metadata_info['descriptiveMetadataUploaded'] \
            and metadata_info['structuralMetadataUploaded']
    return success


def _copy_metadata_files_from_google_drive_to_s3(google_connection, bucket_name, metadata_info, object_id, object_folder_name):  # noqa: E501
    """ copy descriptive and structural metadata from google drive to s3 """
    descriptive_metadata_uploaded = False
    if 'descriptiveMetadataUploaded' in metadata_info:
        descriptive_metadata_uploaded = metadata_info['descriptiveMetadataUploaded']
    if not descriptive_metadata_uploaded:
        descriptive_metadata_file_name = '/tmp/' + object_id + '.xml'
        download_google_file(google_connection, metadata_info['id'], descriptive_metadata_file_name)
        descriptive_metadata_uploaded = _upload_to_s3(bucket_name, descriptive_metadata_file_name,
                                                      object_folder_name + 'descriptive_metadata_mets.xml')
    metadata_info['descriptiveMetadataUploaded'] = descriptive_metadata_uploaded
    structural_metadata_uploaded = False
    if 'structuralMetadataUploaded' in metadata_info:
        structural_metadata_uploaded = metadata_info['structuralMetadataUploaded']
    if not structural_metadata_uploaded:
        repository = metadata_info['repository']
        if repository == 'museum':
            # the museum has one mets file used for both structural and descriptive metadata
            structural_metadata_file_name = descriptive_metadata_file_name
        else:
            raise ValueError('We can only process metadata for the museum at this time.')

        structural_metadata_file_name = '/tmp/' + object_id + '.xml'
        print("copying xml: " + structural_metadata_file_name)
        download_google_file(google_connection, metadata_info['id'], structural_metadata_file_name)
        structural_metadata_uploaded = _upload_to_s3(bucket_name, structural_metadata_file_name,
                                                     object_folder_name + 'structural_metadata_mets.xml')
    metadata_info['structuralMetadataUploaded'] = structural_metadata_uploaded
    # put deletes after copies in case both metadata files are the same file
    delete_file('', descriptive_metadata_file_name)
    delete_file('', structural_metadata_file_name)
    return


def _create_folders_in_s3(bucket_name, config, object_id):
    object_folder_name = config['process-bucket-read-basepath'] + '/' + object_id + '/'
    image_folder_name = object_folder_name + 'images/'
    _create_folder_in_s3(bucket_name, object_folder_name)
    _create_folder_in_s3(bucket_name, image_folder_name)
    return object_folder_name, image_folder_name


def _create_folder_in_s3(bucket_name, directory_name):
    """ Create a folder in s3 """
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket_name, Key=(directory_name))
    return


def _copy_images_from_google_drive_to_s3(google_connection, metadata_info, bucket_name, image_folder_name):
    """ For each image file in list, call a routine to copy each one from Google to S3 """
    all_images_successfully_uploaded = False
    if 'allImagesUploaded' in metadata_info:
        all_images_successfully_uploaded = metadata_info['allImagesUploaded']
    # if images have not already been copied for this object, copy each now
    if not all_images_successfully_uploaded:
        if 'imageList' in metadata_info:
            all_images_successfully_uploaded = True
            for image_info in metadata_info['imageList']:
                if not _copy_image_from_google_drive_to_s3(google_connection, image_info, bucket_name, image_folder_name):  # noqa: E501
                    all_images_successfully_uploaded = False
                if time.time() - start_time > seconds_to_allow_for_processing:
                    all_images_successfully_uploaded = False
                    break
    metadata_info['allImagesUploaded'] = all_images_successfully_uploaded
    return all_images_successfully_uploaded


def _copy_image_from_google_drive_to_s3(google_connection, image_info, bucket_name, image_folder_name):
    """ Download each image, upload it to s3, then delete locally """
    image_successfully_uploaded = False
    if 'imageUploaded' in image_info:
        image_successfully_uploaded = image_info['imageUploaded']
    # if image has not already been copied, copy it now
    if not image_successfully_uploaded:
        file_name = image_info['fileName']
        locale.setlocale(locale.LC_ALL, '')  # Use '' for auto, or force e.g. to 'en_US.UTF-8'
        print("copying image: " + file_name + " (" + "{:n}".format(int(image_info['size'])) + ' bytes)')
        local_folder = '/tmp/'
        local_file_name = local_folder + file_name
        download_google_file(google_connection, image_info['id'], local_file_name)
        s3_remote_file_name = image_folder_name + file_name
        image_successfully_uploaded = _upload_to_s3(bucket_name, local_file_name, s3_remote_file_name)
        delete_file(local_folder, file_name)
        print("Elapsed Time = " + str(int(time.time() - start_time)) + " seconds ")
    image_info['imageUploaded'] = image_successfully_uploaded
    return image_successfully_uploaded


def _upload_to_s3(bucket_name, local_file_name, remote_file_name):
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(local_file_name, bucket_name, remote_file_name)
    except ClientError as e:
        print('Error uploading file ' + local_file_name, e)
        return False
    return True


def _copy_structural_metadata_file_to_s3(bucket_name, local_file_name, s3_object_folder_name):
    success = False
    s3_file_name = s3_object_folder_name + 'structural_metadata_mets.xml'
    success = _upload_to_s3(bucket_name, local_file_name, s3_file_name)
    return success
