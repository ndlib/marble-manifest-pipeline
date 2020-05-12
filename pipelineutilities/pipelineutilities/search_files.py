import boto3
import re
import os
import json
from datetime import datetime, timedelta, timezone
from hashlib import md5
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

    if file_should_be_skipped(file):
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
            url_part = url.netloc
            if url_part == 'rarebooks.nd.edu':
                url_part = 'rarebooks.library.nd.edu'
            return "%s://%s%s/%s" % (url.scheme, url_part, directory, test[0])

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


def file_should_be_skipped(file):
    for exp in skip_files:
        if re.match(exp, file):
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


def crawl_available_files(config):
    order_field = {}
    bucket = config['rbsc-image-bucket']
    print("crawling image files in this bucket: ", bucket)
    for directory in folders_to_crawl:
        objects = get_matching_s3_objects(bucket, directory)
        for obj in objects:
            if is_tracked_file(obj.get('Key')):
                url = bucket_to_url[bucket] + obj.get('Key')
                id = id_from_url(url)

                if id:
                    if not order_field.get(id, False):
                        order_field[id] = {
                            "FileId": id,
                            "Source": "RBSC",
                            "LastModified": False,
                            "files": [],
                        }

                    if not order_field[id]["LastModified"] or obj['LastModified'] > order_field[id]["LastModified"]:
                        order_field[id]["LastModified"] = obj['LastModified']

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

                if id and file['LastModified'] >= time_threshold_for_processing:
                    augement_file_record(file, id, url, config)
                    yield file


def augement_file_record(obj, id, url, config):
    bucket = config['rbsc-image-bucket']

    obj['FileId'] = id
    obj['Label'] = make_label(url, id)
    # Athena timestamp 'YYYY-MM-DD HH:MM:SS' 24 hour time no timezone
    # here i am converting to utc because the timezone is lost,
    obj['LastModified'] = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
    obj['Source'] = 'RBSC'
    obj['Path'] = "s3://" + os.path.join(bucket, obj['Key'])
    obj['SourceUri'] = url
    obj["iiifImageUri"] = os.path.join(config['image-server-base-url'], obj.get('Key'))
    obj["iiifImageFilePath"] = "s3://" + os.path.join(config['image-server-bucket'], obj.get('Key'))


def determine_time_threshold_for_processing(time_in_min):
    """ Creates the datetime object that is used to test all the files against """

    time_threshold_for_processing = datetime.utcnow() - timedelta(minutes=time_in_min)
    # since this is utc already but there is no timezone add it in so
    # the data can be compared to the timze zone aware date in file
    return time_threshold_for_processing.replace(tzinfo=timezone.utc)


def is_tracked_file(file):
    return re.match(r"^.*[.]((jpe?g)|(tif)|(pdf))$", file, re.IGNORECASE)


def output_as_file():
    for row in crawl_available_files().items():
        id = row[0]
        obj = row[1]

        file = "./data/" + md5(id.encode()).hexdigest() + ".json"
        with open(file, 'w') as outfile:
            obj["LastModified"] = obj["LastModified"].strftime('%Y-%m-%d %H:%M:%S')
            json.dump(obj, outfile)


# python -c 'from search_files import *; test()'
def test():
    from pipeline_config import setup_pipeline_config
    event = {"local": True}

    config = setup_pipeline_config(event)
    # change to the prod bucket
    config['rbsc-image-bucket'] = "libnd-smb-rbsc"
    data = list_updated_files(config, 1000000)

    for obj in data:
        print(obj)

    return
