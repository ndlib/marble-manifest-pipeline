import boto3
import re
import os
import json
from hashlib import md5
from urllib.parse import urlparse

# saved live path
# "libnd-smb-rbsc": ["digital/bookreader", "collections/ead_xml/images"]

bucket_to_url = {
    "libnd-smb-rbsc": 'https://rarebooks.library.nd.edu/',
    "rbsc-test-files": 'https://rarebooks.library.nd.edu/',
}

# patterns we skip if the file matches these
skip_files = [
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
    ],
    "moore": [
        r"(^MSN[-]CW[_]8010)"
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
    label = label.replace("-", " ")
    label = label.replace("_", " ")
    label = label.replace(".", " ")
    label = re.sub(' +', ' ', label)
    return label.strip()


def crawl_available_files(config):
    order_field = {}
    for bucket_info in config['rbsc-image-buckets'].items():
        bucket = bucket_info[0]
        for directory in bucket_info[1]:
            objects = get_matching_s3_objects(bucket, directory)
            for obj in objects:
                if is_jpg(obj.get('Key')):
                    url = bucket_to_url[bucket] + obj.get('Key')
                    id = id_from_url(url)
                    if obj.get('Key') == 'digital/civil_war/diaries_journals/images/moore/MSN-CW_8010-01.150.jpg':
                        print(url, id)

                    if id:
                        if not order_field.get(id, False):
                            order_field[id] = {
                                "FileId": id,
                                "Source": "RBSC",
                                "LastModified": False,
                                "files": [],
                            }

                        obj['FileId'] = id
                        obj['Label'] = make_label(url, id)
                        # set the overall last modified to the most recent
                        if not order_field[id]["LastModified"] or obj['LastModified'] > order_field[id]["LastModified"]:
                            order_field[id]["LastModified"] = obj['LastModified']

                        # Athena timestamp 'YYYY-MM-DD HH:MM:SS' 24 hour time no timezone
                        # here i am converting to utc because the timezone is lost,
                        obj['LastModified'] = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                        obj['Order'] = len(order_field[id]['files'])
                        obj['Source'] = 'RBSC'
                        obj['Path'] = "s3://" + os.path.join(bucket, obj['Key'])

                        order_field[id]['files'].append(obj)

    return order_field


def is_jpg(file):
    return re.match("^.*[.]jpe?g$", file, re.IGNORECASE)


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
    from pipeline_config import get_pipeline_config
    event = {"local": True}
    event['local-path'] = "/Users/jhartzle/Workspace/mellon-manifest-pipeline/process_manifest/../example/"

    config = get_pipeline_config(event)
    #data = crawl_available_files(config)
    id = id_from_url("https://rarebooks.nd.edu/digital/civil_war/diaries_journals/images/moore/MSN-CW_8010-01.150.jpg")
    print(id)
    #print(data[id])

    return
