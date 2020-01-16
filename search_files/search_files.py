import boto3
import re
import os
import json
from hashlib import md5
from urllib.parse import urlparse


bucket = "libnd-smb-rbsc"

directories = ['digital/bookreader', 'collections/ead_xml/images']

bucket_to_url = {
    "libnd-smb-rbsc": 'https://rarebooks.library.nd.edu/'
}

# patterns we skip if the file matches these
skip_files = [
    r"^.*[.]100[.]jpg$",
    r"^[.]_.*$",
]

# patterns that corrispond to urls we can parse
valid_urls = [
    r"http[s]?:[/]{2}rarebooks[.]library.*",
]

regexps = {
    "ead_xml": [
        r"([a-zA-Z]{3}-[a-zA-Z]{2}_[0-9]{4}-[0-9]+)",
        r"([a-zA-Z]{3}_[0-9]{2,4}-[0-9]+)",
    ],
    "bookreader": [
        r"(^El_Duende)",
        r"(^Newberry-Case_[a-zA-Z]{2}_[0-9]{3})",
        r"(^.*_(?:[0-9]{4}|[a-zA-Z][0-9]{1,3}))"
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

    for exp in test_expressions:
        test = re.findall(exp, file)
        if test:
            return "%s://%s%s/%s" % (url.scheme, url.netloc, directory, test[0])

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


def crawl_available_files():
    order_field = {}

    for directory in directories:
        objects = get_matching_s3_objects(bucket, directory)
        for obj in objects:
            if is_jpg(obj.get('Key')):
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
    url = "https://rarebooks.library.nd.edu/digital/bookreader/MSN-EA_8011-1-B/images/MSN-EA_8011-01-B-000a.jpg"
    url = "https://rarebooks.nd.edu/digital/civil_war/diaries_journals/images/cline/8007-000a.150.jpg"
    url = "https://rarebooks.library.nd.edu/collections/ead_xml/images/BPP_1001/BPP_1001-214.jpg"
    # url = "https://rarebooks.library.nd.edu/digital/bookreader/CodLat_b04/images/CodLat_b04-000a_front_cover.jpg"

    output_as_file()
    # data = crawl_available_files()

    return


def output_for_ryan():
    data = iter(crawl_available_files())

    output = []
    ids = {}
    ids['colctionator'] = ['2016.10', '2012.105']
    ids['colctionator2'] = ['2017.007', '1992.055']

    for collection_id, items in ids.items():
        for item_id in items:
            row = next(data)
            obj = row[1]
            for file in obj['files']:
                d = {
                    "id": file['Key'],
                    "source": file['Source'],
                    "repository": file['Source'],
                    "filepath": "s3://%s/%s" % (bucket, file['Key']),
                    "sequence": file['Order'],
                    "last_modified": file['LastModified'],
                    "collection_id": collection_id,
                    "item_id": item_id
                }
                output.append(d)

    file = "./file_for_ryan.json"
    with open(file, 'w') as outfile:
        json.dump(output, outfile)
