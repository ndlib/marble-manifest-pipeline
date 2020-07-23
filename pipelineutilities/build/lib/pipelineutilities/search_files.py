import boto3
import re
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

# saved live path
# "libnd-smb-rbsc": ["digital/bookreader", "collections/ead_xml/images"]

bucket_to_url = {
    "libnd-smb-rbsc": 'https://rarebooks.library.nd.edu/',
    "rbsc-test-files": 'https://rarebooks.library.nd.edu/',
}

folders_to_crawl = [
    "digital",
    "collections/ead_xml/images"
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
]

# patterns that corrispond to urls we can parse
valid_urls = [
    r"http[s]?:[/]{2}rarebooks[.]library.*",
    r"http[s]?:[/]{2}rarebooks[.]nd.*",
]

regexps = {
    "ead_xml": [
        r"([a-zA-Z]{3}-[a-zA-Z]{2}_[0-9]{4}-[0-9]+)",
        r"([a-zA-Z]{3}_[0-9]{2,4}-[0-9]+)",
        r"(^[0-9]{4}-[0-9]{2})",
    ],
    "MARBLE-images": [
        r"([a-zA-Z]{3}_[0-9]{9})",
        r"([a-zA-Z]{3}-[a-zA-Z]{3}_[0-9]{4})"
    ],
    "letters": [
        r"(^[0-9]{4}-[0-9]{2})",
    ],
    "colonial_american": [
        r"(^[0-9]{4}-[0-9]{2})",
    ],
    "diaries_journals": [
        r"(^[0-9]{4})",
        r"([a-zA-Z]{3}-[a-zA-Z]{2}_[0-9]{4})",
    ],
    "papers_personal": [
        r"(^[0-9]{4}-[0-9]{2})",
    ],
    "digital": [
        r"(^El_Duende)",
        r"(^Newberry-Case_[a-zA-Z]{2}_[0-9]{3})",
        r"([a-zA-Z]{3}-[a-zA-Z]{2}_[0-9]{4}-[0-9]+)",
        r"(^.*_(?:[0-9]{4}|[a-zA-Z][0-9]{1,3}))",
        r"(^[0-9]{4})",
    ]
}
# urls in this list do not have a group note in the output of the parse_filename function
urls_without_a_group = [
    r"^[a-zA-Z]+_[a-zA-Z][0-9]{2}.*$",  # CodeLat_b04
]


def id_from_url(url):
    if not url_can_be_harvested(url):
        return False

    url = urlparse(url)
    file = os.path.basename(url.path)

    if file_should_be_skipped(url.path):
        return False

    directory = os.path.dirname(url.path)

    test_expressions = []
    for key in regexps:
        if key in url.path:
            test_expressions = regexps[key]
            break

    for exp in test_expressions:
        test = re.findall(exp, file)
        if test:
            return "%s/%s" % (directory, test[0])

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


def crawl_available_files(config):
    order_field = {}
    bucket = config['rbsc-image-bucket']
    print("crawling image files in this bucket: ", bucket)
    for directory in folders_to_crawl:
        objects = get_matching_s3_objects(bucket, directory)
        for obj in objects:
            key = obj.get('Key')
            if is_tracked_file(key):
                url = bucket_to_url[bucket] + key
                id = id_from_url(url)

                if id:
                    obj = _convert_dict_to_camel_case(obj)
                    if not order_field.get(id, False):
                        order_field[id] = {
                            "fileId": id,
                            "sourceType": "S3",
                            "source": bucket,
                            "lastModified": False,
                            "directory": os.path.dirname(key),
                            "files": [],
                        }

                    if not order_field[id]["lastModified"] or obj['lastModified'] > order_field[id]["lastModified"]:
                        order_field[id]["lastModified"] = obj['lastModified']

                    augement_file_record(obj, id, url, config)

                    order_field[id]['files'].append(obj)

    return order_field


def list_updated_files(config: dict, minutes_to_test: int):
    bucket = config['rbsc-image-bucket']
    print("crawling image files in this bucket: ", bucket)
    time_threshold_for_processing = determine_time_threshold_for_processing(minutes_to_test)
    for directory in folders_to_crawl:
        files = get_matching_s3_objects(bucket, directory)
        for file in files:
            if is_tracked_file(file.get('Key')):
                url = bucket_to_url[bucket] + file.get('Key')
                id = id_from_url(url)

                file = _convert_dict_to_camel_case(file)

                if id and file['lastModified'] >= time_threshold_for_processing:
                    augement_file_record(file, id, url, config)
                    yield file


def key_to_id(key):
    return key.lstrip("/").replace("/", "-")


def is_directory(file):
    return file and re.match(".*[/]$", file) and not re.match("^[.]", file)


def augement_file_record(obj, id, url, config):
    bucket = config['rbsc-image-bucket']

    obj['fileId'] = id
    obj['label'] = make_label(url, id)
    obj['sourceType'] = 'S3'
    obj['source'] = bucket
    obj['path'] = "s3://" + os.path.join(bucket, obj['key'])
    obj['sourceUri'] = url
    obj["iiifImageUri"] = os.path.join(config['image-server-base-url'], obj.get('key'))
    obj["iiifImageFilePath"] = "s3://" + os.path.join(config['image-server-bucket'], obj.get('key'))


def determine_time_threshold_for_processing(time_in_min):
    """ Creates the datetime object that is used to test all the files against """

    time_threshold_for_processing = datetime.utcnow() - timedelta(minutes=time_in_min)
    # since this is utc already but there is no timezone add it in so
    # the data can be compared to the timze zone aware date in file
    return time_threshold_for_processing.replace(tzinfo=timezone.utc)


def is_tracked_file(file):
    if file_should_be_skipped(file):
        return False
    return re.match(r"^.*[.]((jpe?g)|(tif)|(pdf))$", file, re.IGNORECASE)


# python -c 'from search_files import *; test()'
def test():
    from pipeline_config import setup_pipeline_config
    event = {"local": True}

    config = setup_pipeline_config(event)
    # change to the prod bucket
    config['rbsc-image-bucket'] = "libnd-smb-rbsc"
    # data = list_updated_files(config, 1000000)
    objs = crawl_available_files(config)
    for key, value in objs.items():
        print(key, value['Directory'])

    return
