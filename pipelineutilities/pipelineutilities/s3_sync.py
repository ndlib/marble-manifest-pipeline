from pathlib import Path
from s3_helpers import upload_file_to_s3
from os.path import join


def s3_sync(s3_bucket: str, s3_root_key: str, local_folder_path: str) -> bool:
    """
    Copy a directory tree from a local folder to an S3 bucket.

    :param s3_bucket: Name of the S3 bucket.
    :param s3_root_key: Name of S3 folder to which directory tree will be copied.
    :param local_folder_path: String of fully qualified local folder whose contents are to be copied to S3.
    """
    success = True
    files = _get_source_files_list(local_folder_path)
    for file in files:
        s3_key = join(s3_root_key, file)
        complete_source_path = join(local_folder_path, file)
        if not upload_file_to_s3(s3_bucket, s3_key, complete_source_path):
            success = False
    return success


def _call_upload_file_to_s3(s3_bucket: str, s3_key: str, complete_source_path: str) -> bool:
    """
    Upload a file to s3.

    :param s3_bucket: Name of the S3 bucket.
    :param s3_root_key: Name of S3 folder to which directory tree will be copied.
    :param complete_source_path: String of fully qualified local file to be copied to S3.

    By creating a separate function, I can mock calls to it to create tests for s3_sync.
    """
    success = True
    try:
        upload_file_to_s3(s3_bucket, s3_key, complete_source_path)
    except FileNotFoundError:
        success = False
    return success


def _get_source_files_list(local_folder_path: str):
    """
    :param local_folder_path:  Root folder for resources you want to list.
    :return: A [str] containing relative names of the files.

    Example:

        /metadata_rules
            /sites
                /inquisitions
                    /rbsc.json
                /marble
                    /aleph.json
                    /embark.json

        >>> sync.list_source_objects("/metadata_rules/sites")
        ['inquisitions/rbsc.json', 'marble/aleph.json', 'marble/embark.json']

    """
    path = Path(local_folder_path)

    files = []

    for file_path in path.rglob("*"):
        if file_path.is_dir():
            continue
        str_file_path = str(file_path)
        str_file_path = str_file_path.replace(f'{str(path)}/', "")
        files.append(str_file_path)

    return files


# python -c 'from s3_sync import *; test()'
def test():
    s3_bucket = "marble-manifest-prod-processbucket-13bond538rnnb"
    s3_root_key = "sites"
    local_folder_path = "/Users/smattiso/git/marble-manifest-pipeline/metadata_rules/sites"
    print("results of get_source_files_list = ", _get_source_files_list(local_folder_path))
    s3_sync(s3_bucket, s3_root_key, local_folder_path)

    # etag_checksum(filename, chunk_size=8 * 1024 * 1024)
