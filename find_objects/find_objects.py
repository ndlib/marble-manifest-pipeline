# find_objects.py
""" Saves a file to a Google Team Drive, in a given parent folder """

import os
import sys
where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am)
sys.path.append(where_i_am + "/dependencies")
from museum_specific_code import get_object_id_given_museum_image_filename, \
    get_object_id_given_museum_metadata_filename  # noqa: E402
from google_utilities import execute_google_query, \
    build_query_string_for_recently_changed_files  # noqa: E402


def find_objects_needing_processed(google_connection, config, repository):
    """ Get a list of objects needing processed, returning json list including object_id """
    objects_needing_processed = []
    objects_needing_processed = _recently_changed_metadata_files(google_connection, config, repository)
    image_files_changed = _recently_changed_image_files(google_connection, config, repository)
    distinct_image_object_ids = _get_distinct_object_ids_given_modified_files_changed_list(image_files_changed)
    distinct_metadata_object_ids = _get_distinct_object_ids_given_modified_files_changed_list(objects_needing_processed)  # noqa: E501
    additional_metadata_object_ids_to_process = _get_additional_metadata_object_ids_to_process(
        distinct_image_object_ids, distinct_metadata_object_ids)
    additional_metadata_files_to_process = []
    if len(additional_metadata_object_ids_to_process) > 0:
        additional_metadata_files_to_process = _find_metadata_files_given_object_id_list(
            google_connection, config, repository, additional_metadata_object_ids_to_process)
    if len(additional_metadata_files_to_process) > 0:
        for metadata_file_to_append in additional_metadata_files_to_process:
            objects_needing_processed.append(metadata_file_to_append)
    objects_needing_processed = _add_repository_to_record(objects_needing_processed, repository)
    return objects_needing_processed


def _add_repository_to_record(objects_needing_processed, repository):
    """ Add repository to each record needing processed """
    for object_needing_processed in objects_needing_processed:
        object_needing_processed['repository'] = repository
    return objects_needing_processed


def _recently_changed_metadata_files(google_connection, config, repository):
    """ Get a list of metadata files that have changed recently """
    metadata_files_changed = []
    drive_id = config['google'][repository]['metadata']['drive-id']
    parent_folder_id = ''
    if 'parent-folder-id' in config['google'][repository]['metadata']:
        parent_folder_id = config['google'][repository]['metadata']['parent-folder-id']
    hours_threshold = config['hours-threshold']
    mime_type = 'xml'
    metadata_files_changed = _recently_changed_team_drive_files(google_connection, drive_id, parent_folder_id, hours_threshold, mime_type)  # noqa: 501
    _add_object_id_to_metadata_files_changed_list(metadata_files_changed, repository)
    return metadata_files_changed


def _recently_changed_image_files(google_connection, config, repository):
    """ Get a list of image files that have changed recently """
    image_files_changed = []
    drive_id = config['google'][repository]['image']['drive-id']
    parent_folder_id = ''
    if 'parent-folder-id' in config['google'][repository]['image']:
        parent_folder_id = config['google'][repository]['image']['parent-folder-id']
    hours_threshold = config['hours-threshold'] * 20  # use for testing to make sure we get some results
    mime_type = 'image'
    image_files_changed = _recently_changed_team_drive_files(google_connection, drive_id, parent_folder_id, hours_threshold, mime_type)  # noqa: 501
    _add_object_id_to_image_files_changed_list(image_files_changed, repository)
    return image_files_changed


def _recently_changed_team_drive_files(google_connection, drive_id, parent_folder_id, hours_threshold, mime_type):
    """ Get a list of metadata files that have changed recently """
    files_changed = []
    query_string = build_query_string_for_recently_changed_files(parent_folder_id, hours_threshold, mime_type)
    files_changed = execute_google_query(google_connection, drive_id, query_string)
    return files_changed


def _get_distinct_object_ids_given_modified_files_changed_list(files_changed_list):
    object_id_set = set([])
    for file_changed in files_changed_list:
        object_id = file_changed['objectId']
        object_id_set.add(object_id)
    return object_id_set


def _get_additional_metadata_object_ids_to_process(image_object_id_set, metadata_object_id_set):
    more_metadata_files_to_retrieve = set([])
    for image_object_id in image_object_id_set:
        if image_object_id not in metadata_object_id_set:
            more_metadata_files_to_retrieve.add(image_object_id)
    return more_metadata_files_to_retrieve


def _find_metadata_files_given_object_id_list(google_connection, config, repository, object_id_list):
    query_string = ''
    file_type = 'metadata'
    drive_id = config['google'][repository][file_type]['drive-id']
    parent_folder_id = ''
    if 'parent-folder-id' in config['google'][repository][file_type]:
        parent_folder_id = config['google'][repository][file_type]['parent-folder-id']
    for object_id in object_id_list:
        if query_string == '':
            query_string = "trashed = False and mimeType contains 'xml' and ("
        else:
            query_string += " or "
        query_string += " name = '" + object_id + ".xml'"
    query_string += ")"
    if parent_folder_id > '':
        query_string += " and '" + parent_folder_id + "' in parents"
    metadata_file_results = execute_google_query(google_connection, drive_id, query_string)
    _add_object_id_to_metadata_files_changed_list(metadata_file_results, repository)
    return metadata_file_results


def _add_object_id_to_image_files_changed_list(files_changed_list, repository):
    success = True
    if repository == 'museum':
        for file_changed in files_changed_list:
            object_id = get_object_id_given_museum_image_filename(file_changed['name'])
            if object_id > '':
                file_changed['objectId'] = object_id
            else:
                print('Unable to get object_id for ' + repository + ' image file ' + file_changed['name'])
                success = False
    else:
        if len(files_changed_list) > 0:
            print('Unable to get object_id for image files associated with repository: ' + repository)
            success = False
    return success


def _add_object_id_to_metadata_files_changed_list(files_changed_list, repository):
    success = True
    if repository == 'museum':
        for file_changed in files_changed_list:
            object_id = get_object_id_given_museum_metadata_filename(file_changed['name'])
            if object_id > '':
                file_changed['objectId'] = object_id
            else:
                print('Unable to get object_id for ' + repository + ' metadata file ' + file_changed['name'])
                success = False
    else:
        if len(files_changed_list) > 0:
            print('Unable to get object_id for metadata files associated with repository: ' + repository)
            success = False
    return success
