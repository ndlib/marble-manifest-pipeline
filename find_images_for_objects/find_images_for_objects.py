# find_images_for_objects.py
""" Saves a file to a Google Team Drive, in a given parent folder """

import time
import os
import sys
where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am)
sys.path.append(where_i_am + "/dependencies")
from xml_processing import get_image_list_from_metadata_file  # noqa: E402
from google_utilities import download_google_file, execute_google_query, \
    get_parent_folders_given_folder_name  # noqa: E402
from file_system_utilities import delete_file, get_full_path_file_name  # noqa: E402

seconds_to_allow_for_processing = 14 * 60


def find_images_for_objects(google_connection, config, objects_needing_processed):
    """ Loop through each of the objects to process, processing each one in turn """
    start_time = time.time()
    for metadata_info in objects_needing_processed:
        # skip processing objects for which we may have already found all images in a previous pass
        if 'allImagesFound' not in metadata_info:
            object_id = ''
            if 'objectId' in metadata_info:
                object_id = metadata_info['objectId']
                print('Finding image information for object id: ' + object_id)
                image_list = _get_image_list_from_metadata_file_id(google_connection, metadata_info['id'], object_id)  # noqa: 501
                repository = metadata_info['repository']
                image_drive_id = config['google'][repository]['image']['drive-id']
                if image_list > []:
                    image_list = _add_image_path_to_image_list(google_connection, image_drive_id, image_list)
                    metadata_info['imageList'] = image_list
                metadata_info['allImagesFound'] = _all_images_have_been_found_for_object_id(metadata_info)
                if not metadata_info['allImagesFound']:
                    print("Image(s) Missing for " + metadata_info)
                if (time.time() - start_time) > (seconds_to_allow_for_processing):
                    print("Time expired. Saving objects still needing processed and exiting")
                    break
                print("Elapsed_Time = " + str(int(time.time() - start_time)) + " seconds.")
            else:
                # print(metadata_info)
                metadata_file_name = 'NULL'
                if 'name' in metadata_info:
                    metadata_file_name = metadata_info['name']
                print("Unable to find objectID in metadata_info: " + metadata_file_name + ' - unable to process file.')  # noqa: 501
    return objects_needing_processed


def _get_image_list_from_metadata_file_id(google_connection, file_id, object_id):
    """ Get an array of image files from within a metadata file """
    folder_name = '/tmp/'
    file_name = object_id + '.xml'
    full_path_file_name = get_full_path_file_name(folder_name, file_name)
    download_google_file(google_connection, file_id, full_path_file_name)
    image_file_list = get_image_list_from_metadata_file(full_path_file_name)
    delete_file(folder_name, file_name)
    return image_file_list


def _add_image_path_to_image_list(google_connection, drive_id, image_list):
    query_string = ''
    file_name_including_path = ''
    if len(image_list) > 0:
        file_name_including_path = image_list[0]['filePath']
    parent_folder_query_phrase = _get_parent_folder_query_phrase(google_connection, drive_id, file_name_including_path)
    for image in image_list:
        if query_string == '':
            query_string = "trashed = False and mimeType contains 'image' and ("
        else:
            query_string += " or "
        query_string += " name = '" + image['fileName'] + "'"
    query_string += ")"
    query_string += parent_folder_query_phrase
    image_file_results = execute_google_query(google_connection, drive_id, query_string)
    for image_file_result in image_file_results:
        for image in image_list:
            if image['fileName'] == image_file_result['name']:
                image['id'] = image_file_result['id']
                image['mimeType'] = image_file_result['mimeType']
                image['parents'] = image_file_result['parents']
                image['driveId'] = image_file_result['driveId']
                image['size'] = image_file_result['size']
    return image_list


def _all_images_have_been_found_for_object_id(enhanced_metadata_info):
    all_images_found_flag = True
    if 'imageList' not in enhanced_metadata_info:
        all_images_found_flag = False
    for image_record in enhanced_metadata_info['imageList']:
        if 'id' not in image_record:
            all_images_found_flag = False
            break
    return all_images_found_flag


def _get_parent_folder_query_phrase(google_connection, drive_id, file_path):
    """ create parent folder phrase for google query given file path with "/" format """
    query_string = ''
    parent_folder_name = _parse_file_path_to_get_parent_folder_name(file_path)
    parent_folders = get_parent_folders_given_folder_name(google_connection, drive_id, parent_folder_name)
    for parent_folder in parent_folders:
        if query_string == '':
            query_string = " and ("
        else:
            query_string += " or "
        query_string += "'" + parent_folder['id'] + "' in parents"
    query_string += ")"
    return query_string


def _parse_file_path_to_get_parent_folder_name(file_path):
    """ Assume format separated by "/".  Chunk before last "/" is parent folder name """
    parent_folder_name = ''
    while '/' in file_path:
        loc = file_path.find('/')
        parent_folder_name = file_path[:loc]
        file_path = file_path[loc + 1:]
    return parent_folder_name
