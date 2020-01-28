import csv
from file_system_utilities import create_directory, get_full_path_file_name


def write_csv_header(local_path, file_name, header_fields):
    """ Write CSV header given a list of fields to write """
    create_directory(local_path)
    fully_qualified_file_name = get_full_path_file_name(local_path, file_name)
    with open(fully_qualified_file_name, 'w') as outcsv:
        writer = csv.DictWriter(outcsv, fieldnames=header_fields)
        writer.writeheader()
    return


def append_to_csv(local_path, file_name, header_fields, dict_to_write, intentionally_excluded_fields):
    """ Append a line to the csv file we've already started """
    create_directory(local_path)
    fully_qualified_file_name = get_full_path_file_name(local_path, file_name)
    my_dict = _remove_undefined_nodes(dict_to_write.copy(), header_fields, intentionally_excluded_fields)
    with open(fully_qualified_file_name, 'a') as outcsv:
        writer = csv.DictWriter(outcsv, fieldnames=header_fields)
        writer.writerow(my_dict)
    return


def _remove_undefined_nodes(dict, header_fields, intentionally_excluded_fields):
    """ Remove nodes from dictionary that don't correspond to named fields to be saved. """
    my_dict = dict.copy()
    nodes_to_delete = []
    for key in my_dict:
        if key not in header_fields:
            if key not in intentionally_excluded_fields:
                print("key not in header fields: ", key)  # may need to capture into sentry?
            nodes_to_delete.append(key)
    for node in nodes_to_delete:
        my_dict.pop(node, None)
    return my_dict
