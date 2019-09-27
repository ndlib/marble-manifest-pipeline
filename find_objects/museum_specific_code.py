# museum_specific_code.py
""" Code here applies only to museum images or metadata """


def get_object_id_given_museum_metadata_filename(metadata_filename):
    """ Get object id given museum metadata filename by truncating ".xml"  """
    object_id = ''
    if metadata_filename > '':
        object_id = metadata_filename.replace('.xml', '')
    return object_id


def get_object_id_given_museum_image_filename(image_filename):
    """ Get object id given pattern from museum image filename
        by truncating starting at  "-v" in the filename, then replacing "_" with "."  """
    object_id = ''
    v_loc = image_filename.find('-v')
    if v_loc > -1:
        # truncate from "-v" to the end of the string, then replace "_" with "."
        object_id = image_filename[:v_loc].replace('_', '.')
        # Make sure we don't return a value for an oddball like: 1924_001_069-ref-v0001.jpg
        if '-' in object_id:
            object_id = ''
    return object_id
#
#
# def guess_museum_image_folder_name_given_object_id(object_id):
#     """ Museum Object_IDs have dots, folders have underscores.
#         Guess the museum image folder name by replacing "." with '_'. """
#     folder_name = object_id.replace('.', '_')
#     return folder_name
#
#
# def guess_museum_image_folder_name_given_bad_folder_name(bad_folder_name):
#     """ Truncate folder name beyond right-most underscore character """
#     new_folder_name = ''
#     while '_' in bad_folder_name:
#         u_loc = bad_folder_name.find('_')
#         if new_folder_name > "":
#             new_folder_name += "_"
#         new_folder_name += bad_folder_name[:u_loc]
#         bad_folder_name = bad_folder_name[u_loc + 1:]
#     return new_folder_name


# python -c 'from src.museum_specific_code import *; test()'
def test():
    get_object_id_given_museum_metadata_filename('1899_03.xml')
    get_object_id_given_museum_image_filename('1899_029_f-v0001.jpg')
