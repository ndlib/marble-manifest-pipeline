import boto3
import re
import os
from urllib.parse import urlparse

bucket = "libnd-smb-rbsc"

# patterns we skip if the file matches these
skip_files = [
    r"^.*[.]100[.]jpg$",
    r"^[.]_.*$",
]

# patterns that corrispond to urls we can parse
valid_urls = [
    r"http[s]?:[/]{2}rarebooks[.]library.*",
]

# these are used to match the id part of the file name
regexps = {
    "three_part_underscore": r"^([0-9]{4}_[0-9]{2}_[0-9]{2}[-.]?)(.*)$",
    "three_part_dash_underscore": r"^([a-zA-Z0-9]{3}-[a-zA-Z]{2}_[0-9]{2,4}[-.]?)(.*)?$",
    "two_part_underscore": r"^([a-zA-Z0-9]{3,8}_[a-zA-Z0-9]{2,4}[-.]?)(.*)?$",
    "one_part_underscore": r"^([a-zA-Z0-9]{3,8}[-.]?)(.*)?$",
}

# urls in this list do not have a group note in the output of the parse_filename function
urls_without_a_group = [
    r"^[a-zA-Z]+_[a-zA-Z][0-9]{2}.*$",  # CodeLat_b04
]


def parse_filename(file):
    """
    Returns a hash of the split filename
    id: The first part of the file that loosely connects to the directory name.
    group: if there is a sub grouping i.e. by archival folder it is listed here.
    label:  the label that should be applied to the file

    :param file: File to parse
    """
    found = 0
    found_keys = []
    file = os.path.basename(file)
    file = re.sub("[.]jpg$", "", file)
    file = re.sub("[.]150$", "", file)
    output = {"id": '', "group": '', "label": ''}

    for key in regexps:
        if re.match(regexps[key], file):
            found += 1
            found_keys.append(key)

    if (len(found_keys) != 0):
        matches = re.match(regexps[found_keys[0]], file)
        output['id'] = matches[1]
        removed_key = file.replace(matches[1], '')

    has_group = True
    for exp in urls_without_a_group:
        if re.match(exp, file):
            has_group = False

    # it is a group match only if the first group has numbers and no letters.
    if has_group:
        matches = re.match(r"^([0-9]+).*", removed_key)
        output['group'] = matches[1]
        removed_key = removed_key.replace(matches[1], '')

    removed_key = removed_key.replace("-", " ")
    removed_key = removed_key.replace("_", " ")
    removed_key = removed_key.replace(".", " ")
    removed_key = re.sub(' +', ' ', removed_key)
    output['label'] = removed_key.strip()

    return output


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


def getFilesFromUri(url):
    """
    Returns an iterator of the files for a url in s3

    :param url: The url to search for files
    """
    if not url_can_be_harvested(url):
        return False

    url = urlparse(url)
    directory = os.path.dirname(url.path)[1:]

    base_directory = parse_filename(url.path)

    directory = directory + "/"
    result = get_matching_s3_objects(bucket, directory)
    for obj in result:
        # make sure it is a jpg
        if re.match("^.*[.]jpg$", obj.get('Key'), re.MULTILINE):
            file = os.path.basename(obj.get('Key'))

            if not file_should_be_skipped(file):
                output = parse_filename(file)
                if output['group'] == base_directory['group']:
                    obj['Label'] = output['label']
                    obj['group'] = output['group']
                    yield obj


# python -c 'from search_files import *; test()'
def test():
    url = "https://rarebooks.library.nd.edu/digital/bookreader/MSN-EA_8011-1-B/images/MSN-EA_8011-01-B-000a.jpg"
    url = "https://rarebooks.nd.edu/digital/civil_war/diaries_journals/images/cline/8007-000a.150.jpg"
    url = "https://rarebooks.library.nd.edu/collections/ead_xml/images/BPP_1001/BPP_1001-214.jpg"
    url = "https://rarebooks.library.nd.edu/digital/bookreader/CodLat_b04/images/CodLat_b04-000a_front_cover.jpg"

    for obj in getFilesFromUri(url):
        print(obj['Key'])
