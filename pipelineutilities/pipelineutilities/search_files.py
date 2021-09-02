import boto3
import re
import os
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse
# saved live path
# "libnd-smb-rbsc": ["digital/bookreader", "collections/ead_xml/images"]

bucket_to_url = {
    # "libnd-smb-rbsc": 'https://rarebooks.library.nd.edu/',
    # "rbsc-test-files": 'https://rarebooks.library.nd.edu/',
    "libnd-smb-marble": "https://marbleb-multimedia.library.nd.edu/",
    "mlk-multimedia-333680067100": 'https://mlk-multimedia.library.nd.edu/',
    "steve-multimedia-333680067100": 'https://steve-multimedia.libraries.nd.edu/',
    "sm-multimedia-333680067100": 'https://sm-multimedia.libraries.nd.edu/',
    "marble-multimedia-333680067100": 'https://marble-multimedia.library.nd.edu/',
    "marble-multimedia-test-333680067100": 'https://marble-multimedia-test.library.nd.edu/',
    "marble-multimedia-230391840102": 'https://marble-multimedia.library.nd.edu/',
    "marble-multimedia-test-230391840102": 'https://marble-multimedia-test.library.nd.edu/',
    "marbleb-multimedia-230391840102": 'https://marble-multimedia.library.nd.edu/',
    "marbleb-multimedia-test-230391840102": 'https://marble-multimedia-test.library.nd.edu/',
}

folders_to_crawl = [
    # "digital",
    # "collections/ead_xml/images",
    # "audio",
    # "video"
    "public-access",
    "Aleph",
    "ArchivesSpace",
    "Curate",
    # "other"
]

# patterns we skip if the file matches these
skip_files = [
    r"^.*[.]072[.]jpg$",
    r"^.*[.]100[.]jpg$",
    r"^[.]_.*$",
    r"^_.*$",
]

# patterns we skip if the folder matches these
skip_folders = [
    r"^.*resource.frk.*$",
    r"^.*resourc[0-9].frk.*$",
]

# patterns that corrispond to urls we can parse
valid_urls = [
    r"http[s]?:[/]{2}rarebooks[.]library.*",
    r"http[s]?:[/]{2}rarebooks[.]nd.*",
    r"http[s]?:[/]{2}.*-multimedia[.]library.*",
    r"http[s]?:[/]{2}.*-multimedia[.]nd.*",
]

regexps = {
    # "ead_xml": [
    #     r"([a-zA-Z]{3}-[a-zA-Z]{2}_[0-9]{4}-[0-9]+)",
    #     r"([a-zA-Z]{3}_[0-9]{2,4}-[0-9]+)",
    #     r"(^[0-9]{4}-[0-9]{2})",
    # ],
    # "MARBLE-images": [
    #     r"([a-zA-Z]{3}_[0-9]{9})",
    #     r"([a-zA-Z]{3}-[a-zA-Z]{3}_[0-9]{4})",
    #     r"([a-zA-Z]{3}-[a-zA-Z]{3}_[0-9]{3}-[0-9]{3})",
    #     r"(^[a-zA-Z]{4}_[0-9]{4}-[0-9]{2})",
    # ],
    # "letters": [
    #     r"(^[0-9]{4}-[0-9]{2})",
    # ],
    # "colonial_american": [
    #     r"(^[0-9]{4}-[0-9]{2})",
    # ],
    # "diaries_journals": [
    #     r"(^[0-9]{4})",
    #     r"([a-zA-Z]{3}-[a-zA-Z]{2}_[0-9]{4})",
    # ],
    # "papers_personal": [
    #     r"(^[0-9]{4}-[0-9]{2})",
    # ],
    # "digital": [
    #     r"(^El_Duende)",
    #     r"(^Newberry-Case_[a-zA-Z]{2}_[0-9]{3})",
    #     r"([a-zA-Z]{3}-[a-zA-Z]{2}_[0-9]{4}-[0-9]+)",
    #     r"(^.*_(?:[0-9]{4}|[a-zA-Z][0-9]{1,3}))",
    #     r"(^[0-9]{4})",
    # ],
    # "audio": [
    #     r"/([^/]*)/[^/]*\.mp3",  # Gets the directory the .mp3 is in
    #     r"/([^/]*)/[^/]*\.wav",  # Gets the directory the .wav is in
    # ],
    # "video": [
    #     r"/([^/]*)/[^/]*\.mp4",  # Gets the directory the .mp4 is in
    # ],

    "Aleph": [
        r"([a-zA-Z]{3}_[0-9]{9})",
        r"([a-zA-Z]{3}-[a-zA-Z]{3}_[0-9]{4})",
        r"([a-zA-Z]{3}-[a-zA-Z]{3}_[0-9]{3}-[0-9]{3})",
        r"(^[a-zA-Z]{4}_[0-9]{4}-[0-9]{2})",
    ],
    "ArchivesSpace": [
        r"(^[a-zA-Z0-9]+-[a-zA-Z0-9]+_[a-zA-Z0-9]+-[0-9]+)",
        r"(^[a-zA-Z0-9]+-[a-zA-Z0-9]+_[a-zA-Z0-9]+)",
        r"(^[a-zA-Z]{3}_[a-zA-Z0-9]+)",
        r"(^[a-zA-Z]{4}_[0-9]{4}-[0-9]+)",
    ],
    "Curate": [
        r"/([^/]*)/([^/]*)/[^/]*\.tiff?",
    ],
    "other": [
        r"([a-zA-Z]{3}_[0-9]{9})",
        r"([a-zA-Z]{3}-[a-zA-Z]{3}_[0-9]{4})",
        r"([a-zA-Z]{3}-[a-zA-Z]{3}_[0-9]{3}-[0-9]{3})",
        r"(^[a-zA-Z]{4}_[0-9]{4}-[0-9]{2})",
    ],
    "public-access": [  # assumption is that format will be: public-access/media/parentId/filename  we want to return media/parentId/filename, and will chose parentId as GroupId
        r"/([^/]*)/([^/]*)/[^/]*\.mp4",  # Gets the directory the file is in
        r"/([^/]*)/([^/]*)/[^/]*\.mp3",  # Gets the directory the file is in
        r"/([^/]*)/([^/]*)/[^/]*\.wav",  # Gets the directory the file is in
        r"/([^/]*)/([^/]*)/[^/]*\.pdf",  # Gets the directory the file is in
    ],
}

# Regexps for these folders should use the full path as input instead of just the filename
full_path_folders = [
    # "audio",
    # "video",
    "public-access"
]

# urls in this list do not have a group note in the output of the parse_filename function
urls_without_a_group = [
    r"^[a-zA-Z]+_[a-zA-Z][0-9]{2}.*$",  # CodeLat_b04
]


folder_exposed_through_cdn = 'public-access'


def id_from_url(url):
    if not url_can_be_harvested(url):
        return False

    url = urlparse(url)
    file = os.path.basename(url.path)
    if file_should_be_skipped(url.path):
        return False

    test_expressions = []
    use_full_path = False
    for key in regexps:
        if key in url.path:
            test_expressions = regexps[key]
            if key in full_path_folders:
                use_full_path = True
            break

    for exp in test_expressions:
        test = re.search(exp, url.path if use_full_path else file)
        if test:
            if use_full_path:
                return test.group(2)
            return test.group(1)

    return False


def get_matching_s3_objects(bucket, prefix="", suffix=""):
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {'Bucket': bucket}

    # We can pass the prefix directly to the S3 API.  If the user has passed
    # a tuple or list of prefixes, we go through them one by one.
    if isinstance(prefix, str):
        prefixes = (prefix, )
    else:
        prefixes = prefix

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix
        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                return
            for obj in contents:
                key = obj["Key"]
                if key.endswith(suffix):
                    yield obj


def url_can_be_harvested(url):
    for exp in valid_urls:
        if re.match(exp, url):
            return True
    return False


def file_should_be_skipped(file_path):
    file_name = os.path.basename(file_path)
    folder_name = os.path.dirname(file_path)
    for exp in skip_files:
        if re.match(exp, file_name):
            return True
    for exp in skip_folders:
        if re.match(exp, folder_name):
            return True
    return False


def make_label(url, id):
    label = url.replace(id, "")
    label = label.replace(".jpg", "")
    label = label.replace(".tif", "")
    label = label.replace(".tiff", "")
    label = label.replace(".mp3", "")
    label = label.replace(".mp4", "")
    label = label.replace(".wav", "")
    label = label.replace("-", " ")
    label = label.replace("_", " ")
    label = label.replace(".", " ")
    label = re.sub(' +', ' ', label)
    return label.strip()


def _convert_dict_to_camel_case(obj: dict) -> dict:
    keys_to_remove = []
    for k, v in dict(obj).items():
        if re.match("^[A-Z]{1}.*", k):
            obj[k[0].lower() + k[1:]] = v
            keys_to_remove.append(k)
    for k in keys_to_remove:
        del obj[k]
    return obj


def get_url_from_bucket_plus_key(bucket: str, key: str) -> str:
    """ Added to simplify creating url from key, especially in testing environment """
    if bucket in bucket_to_url:
        return bucket_to_url[bucket] + key
    else:
        return bucket_to_url["marbleb-multimedia-230391840102"] + key


def crawl_available_files(config: dict, bucket: str):
    order_field = {}
    print("crawling image files in this bucket: ", bucket)
    for directory in folders_to_crawl:
        objects = get_matching_s3_objects(bucket, directory)
        for obj in objects:
            key = obj.get('Key')
            if is_tracked_file(key):
                url = get_url_from_bucket_plus_key(bucket, key)
                id = id_from_url(url)

                if id:
                    obj = _convert_dict_to_camel_case(obj)
                    if 'eTag' in obj:
                        obj['eTag'] = obj['eTag'].replace('"', '')  # strip duplicated quotes: {'ETag': '"8b50cfed39b7d8bcb4bd652446fe8adf"'}  # noqa: E501
                    if not order_field.get(id, False):
                        order_field[id] = {
                            "fileId": id,
                            "sourceType": "S3",
                            "source": bucket,
                            "lastModified": False,
                            "directory": os.path.dirname(key),
                            "files": [],
                        }

                    last_modified_iso = obj['lastModified'].isoformat()
                    obj['lastModified'] = obj['lastModified'].isoformat()
                    if not order_field[id]["lastModified"] or last_modified_iso > order_field[id]["lastModified"]:
                        order_field[id]["lastModified"] = last_modified_iso

                    augement_file_record(obj, id, url, config, bucket)

                    order_field[id]['files'].append(obj)
    return order_field


def list_updated_files(config: dict, bucket: str, minutes_to_test: int):
    print("crawling image files in this bucket: ", bucket)
    time_threshold_for_processing = determine_time_threshold_for_processing(minutes_to_test).isoformat()
    for directory in folders_to_crawl:
        files = get_matching_s3_objects(bucket, directory)
        for file in files:
            if is_tracked_file(file.get('Key')):
                url = get_url_from_bucket_plus_key(bucket, file.get('Key'))
                id = id_from_url(url)

                file = _convert_dict_to_camel_case(file)

                if id and file['lastModified'].isoformat() >= time_threshold_for_processing:
                    augement_file_record(file, id, url, config, bucket)
                    yield file


def list_all_files(config: dict, bucket: str):
    print("crawling image files in this bucket: ", bucket)
    for directory in folders_to_crawl:
        objects = get_matching_s3_objects(bucket, directory)
        for obj in objects:
            if is_tracked_file(obj.get('Key')):
                url = get_url_from_bucket_plus_key(bucket, obj.get('Key'))
                id = key_to_id(obj.get('Key'))
                augement_file_record(obj, id, url, config, bucket)

                yield obj


def list_all_directories(config: dict, bucket: str):
    order_field = {}
    print("crawling image files in this bucket: ", bucket)
    for directory in folders_to_crawl:
        objects = get_matching_s3_objects(bucket, directory)
        for obj in objects:
            if is_tracked_file(obj.get('Key')):
                key = obj.get('Key')
                url = get_url_from_bucket_plus_key(bucket, key)
                if is_directory(key):
                    directory = key
                else:
                    directory = os.path.dirname(key)
                directory_id = key_to_id(directory)

                id = id_from_url(url)

                if id:
                    id = key_to_id(id)
                    if not order_field.get(directory_id, False):
                        order_field[directory_id] = {
                            "id": directory_id,
                            "path": directory,
                            "objects": {},
                        }

                    if not order_field[directory_id]['objects'].get(id, False):
                        order_field[directory_id]['objects'][id] = {
                            "id": id,
                            "path": directory,
                            "label": id.replace(directory_id, "").ltrim("-").replace("-", " "),
                            "directory_id": directory,
                            "Source": "RBSC" if bucket == config['rbsc-image-bucket'] else "Multimedia",
                            "LastModified": False,
                            "files": [],
                        }

                    last_modified_iso = obj['LastModified'].isoformat()
                    obj['LastModified'] = obj['LastModified'].isoformat()
                    if not order_field[directory_id]['objects'][id]["LastModified"] or last_modified_iso > order_field[directory_id]['objects'][id]["LastModified"]:
                        order_field[directory_id]['objects'][id]["LastModified"] = last_modified_iso

                    augement_file_record(obj, id, url, config, bucket)

                    order_field[directory_id]['objects'][id]['files'].append(obj)

    return order_field


def key_to_id(key):
    return key.lstrip("/").replace("/", "-")


def is_directory(file):
    return file and re.match(".*[/]$", file) and not re.match("^[.]", file)


def augement_file_record(obj: dict, id: str, url: str, config: dict, bucket: str) -> dict:
    ''' Note:  This was changed 7/2/2021 to rename *.pdf to *.tif so the Marble image processor will deal with pdfs correctly. '''
    obj['fileId'] = id
    obj['label'] = make_label(url, id)
    obj['sourceType'] = 'S3'
    obj['source'] = bucket
    obj['path'] = obj['key'].replace('public-access/', '')
    obj["sourceBucketName"] = bucket
    obj["sourceFilePath"] = obj.get('key')
    in_cdn_folder_flag = folder_exposed_through_cdn in obj.get('key')
    file_extension = os.path.splitext(obj.get('key'))[1].lower()
    if bucket == config.get('marble-content-bucket') and in_cdn_folder_flag and is_media_file(config.get('media-file-extensions', []), obj.get('key')):
        # references to this file will only be through the CDN, not through S3
        obj['filePath'] = obj.get('key').replace('public-access/', '')
        obj['sourceType'] = 'Uri'
        obj['mediaGroupId'] = id
        obj['mediaServer'] = config['media-server-base-url']
        obj['mediaResourceId'] = obj.get('filePath').replace('/', '%2F')
        obj['sourceUri'] = os.path.join(obj.get('mediaServer'), obj.get('mediaResourceId'))
        obj['typeOfData'] = 'Multimedia bucket'
    elif (not in_cdn_folder_flag) and (file_extension in config['image-file-extensions'] or file_extension == '.pdf'):
        file_path_without_extension = os.path.splitext(obj.get('key'))[0].replace('public-access/', '')
        obj['filePath'] = obj.get('key')
        if file_extension in ('.jpg', '.tif'):
            obj['filePath'] = file_path_without_extension + '.tif'
        if file_extension == '.pdf':
            update_pdf_fields(obj)
            file_extension = '.pdf'
        obj['objectFileGroupId'] = id
        obj['imageGroupId'] = id
        obj['mediaServer'] = config['image-server-base-url']
        file_path_no_extension = os.path.join(Path(obj.get('filePath')).parent, Path(obj.get('filePath')).stem)
        obj['mediaResourceId'] = file_path_no_extension.replace('/', '%2F')
    else:
        obj = {}
    if obj and file_extension and 'mimeType' not in obj:
        obj['mimeType'] = _get_mime_type_given_file_extension(file_extension)
    return obj


def determine_time_threshold_for_processing(time_in_min):
    """ Creates the datetime object that is used to test all the files against """

    time_threshold_for_processing = datetime.utcnow() - timedelta(minutes=time_in_min)
    # since this is utc already but there is no timezone add it in so
    # the data can be compared to the timze zone aware date in file
    return time_threshold_for_processing.replace(tzinfo=timezone.utc)


def is_tracked_file(file):
    if file_should_be_skipped(file):
        return False
    return re.match(r"^.*[.]((jpe?g)|(tiff?)|(pdf)|(wav)|(mp[34]))$", file, re.IGNORECASE)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def _get_mime_type_given_file_extension(file_extension: str) -> str:
    if not file_extension:
        return ""
    if file_extension in ['.tif', '.tiff']:
        return 'image/tiff'
    elif file_extension in ['.pdf']:
        return 'application/pdf'
    elif file_extension in ['.mp3']:
        return 'audio/mpeg'
    elif file_extension in ['.mp4']:
        return 'video/mp4'
    elif file_extension in ['.wav']:
        return 'audio/wav'
    return ''


def is_media_file(media_file_extensions: list, file_name: str) -> bool:
    """ If the file extension is in a list of media_file_extensions, then return True (this is a media file), else return False """
    file_extension = Path(file_name).suffix
    if file_extension in media_file_extensions:
        return True
    return False


def update_pdf_fields(standard_json: dict):
    fields = ['id', 'filePath', 'description', 'title', 'path']
    for field in fields:
        if field in standard_json:
            standard_json[field] = standard_json.get(field).replace('.pdf', '.tif')
    standard_json['mimeType'] = 'image/tiff'  # correct the mimeType to reflect tiff


# python -c 'from search_files import *; test()'
def test():
    from pipeline_config import setup_pipeline_config
    event = {"local": True}

    config = setup_pipeline_config(event)
    # change to the prod bucket
    # config['rbsc-image-bucket'] = "libnd-smb-rbsc"
    # config['multimedia-bucket'] = "marble-multimedia-230391840102"
    config['marble-content-bucket'] = "libnd-smb-marble"
    # data = list_updated_files(config, config['marble-content-bucket'], 1000000)
    data = crawl_available_files(config, config['marble-content-bucket'])
    for id, value in data.items():
        print("results =", id)
        # print(value)

    return
