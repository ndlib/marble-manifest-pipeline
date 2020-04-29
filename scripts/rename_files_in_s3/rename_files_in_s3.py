# rename_s3_files.py
import _set_script_path # noqa
import os
import sys
import boto3
from search_files import get_matching_s3_objects


def rename_files_in_s3(bucket_name: str, folder_path: str, replacement_pattern: str, replacement_value: str):
    objects = get_matching_s3_objects(bucket_name, folder_path)
    for obj in objects:
        if obj['Key'] != folder_path:
            old_key = obj['Key']
            new_path = os.path.split(old_key)
            new_file = new_path[1].replace(replacement_pattern, replacement_value)
            new_key = new_path[0] + "/" + new_file
            rename_s3_file(bucket_name, old_key, new_key)


def rename_s3_file(bucket_name, old_file_key, new_file_key):
    s3 = boto3.resource('s3')
    # Copy from old_file_name to new_file_name
    s3.Object(bucket_name, new_file_key).copy_from(os.join(bucket_name, old_file_key))
    s3.Object(bucket_name, old_file_key).delete()


# files to rename
# https://rarebooks.library.nd.edu/digital/MARBLE-images/BOO_001016383/001016383.pdf
# https://rarebooks.library.nd.edu/digital/MARBLE-images/BOO_004476467/004476467_001.tif
# https://rarebooks.library.nd.edu/digital/MARBLE-images/BOO_004607040/004607040_001.tif
# https://rarebooks.library.nd.edu/digital/MARBLE-images/BOO_004607050/004607050_001.tif
# https://rarebooks.library.nd.edu/digital/MARBLE-images/BOO_004607047/004607047_001.tif
# python -c 'from rename_s3_files import *; test()'
def test():
    """ test exection """
    rename_files_in_s3("libnd-smb-rbsc", "digital/MARBLE-images/BOO_001016383/", "_001016383", "thing")


if __name__ == "__main__":
    try:
        bucket = sys.argv[1]
        folder_name = sys.argv[2]
        replacement_pattern = sys.argv[3]
        replacement_value = sys.argv[4]

        rename_files_in_s3(bucket, folder_name, )

    except Exception as e:
        print(e)
