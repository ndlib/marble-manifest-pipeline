# get_manifest_info.py
""" Retrieves necessary info from Manifest """

import json
from urllib import request, error
import re
import boto3


def append_manifest_info(manifest_id, manifest_json):
    """ Retrieves manifest to harvest key important information """
    manifest_info = {}
    manifest_info['manifest_found'] = (manifest_json != {})
    manifest_info['id'] = manifest_id
    if manifest_info['manifest_found']:
        manifest_info = _harvest_important_manifest_info(manifest_json, manifest_info)
        manifest_info['manifest_url'] = _build_item_manifest_url_given_id(manifest_id)
    else:
        manifest_info['repository'] = _get_manifest_repository({})
    manifest_info['library'] = _get_library(manifest_info)
    manifest_info['display_library'] = _get_display_library(manifest_info)
    return manifest_info


def _get_id_given_manifest_url(manifest_url):
    id = ""
    # URL is expected to be of the form:  "https://presentation-iiif.library.nd.edu/ils-000909884/manifest"
    if manifest_url.startswith('https://presentation-iiif.library.nd.edu/'):
        id = re.sub('https://presentation-iiif.library.nd.edu/', '', manifest_url)  # strip root url
        id = re.sub('/manifest', '', id)  # strip manifest portion of url if it applies
        id = re.sub('collection/', '', id)  # strip collection portion of url if it applies
    return id


def _harvest_important_manifest_info(manifest_json, manifest_info):
    try:
        if '@id' in manifest_json:
            manifest_info['url'] = manifest_json['@id']
        if 'thumbnail' in manifest_json:
            if '@id' in manifest_json['thumbnail']:
                manifest_info['thumbnail'] = manifest_json['thumbnail']['@id']
        manifest_info['title'] = _get_manifest_title(manifest_json)
        manifest_info['creator'] = _get_manifest_creator(manifest_json)
        manifest_info['description'] = _get_manifest_description(manifest_json)
        manifest_info['classification'] = _get_manifest_classification(manifest_json)
        manifest_info['repository'] = _get_manifest_repository(manifest_json)
    except KeyError:
        raise
    return manifest_info


def _get_manifest_repository(manifest_json):
    repository = ""
    logo = _get_manifest_logo(manifest_json)
    if 'Archive'.upper() in logo.upper():
        repository = 'UNDA'  # Primo expects Repository of UNDA for Archives
    if 'RBSC'.upper() in logo.upper():
        repository = 'SPEC'  # Primo expects Repository of SPEC for RBSC
    if 'Snite'.upper() in logo.upper():
        repository = 'SNITE'
    if repository == '':
        repository = 'SNITE'
    return repository


def _get_library(manifest_info):
    library = ""
    if manifest_info['repository'].upper() == 'UNDA'.upper():
        library = 'HESB'
    if manifest_info['repository'].upper() == 'SPEC'.upper():
        library = 'HESB'
    if manifest_info['repository'].upper() == 'Snite'.upper():
        library = 'Snite'
    if library == "":
        library = 'Snite'
    return library


def _get_display_library(manifest_info):
    display_library = ""
    if manifest_info['library'].upper() == 'UNDA'.upper():
        display_library = 'University Archives'
    if manifest_info['library'].upper() == 'SPEC'.upper():
        display_library = 'Rare Books and Special Collections'
    if manifest_info['library'].upper() == 'Snite'.upper():
        display_library = 'Snite Museum of Art'
    if display_library == "":
        display_library = 'Snite'
    return display_library


def _get_manifest_logo(manifest_json):
    """ Get Logo from Manifest JSON [metadata][Creator] """
    logo = ""
    if 'logo' in manifest_json:
        if '@id' in manifest_json['logo']:
            logo = manifest_json['logo']['@id']
    return logo


def _get_manifest_title(manifest_json):
    """ Get Author from Manifest JSON [metadata][Creator] """
    title = ""
    if 'label' in manifest_json:
        title = manifest_json['label']
    else:
        if 'metadata' in manifest_json:
            for metadata_item in manifest_json['metadata']:
                if metadata_item['label'].upper() == 'Title'.upper():
                    title = metadata_item['value']
                if title != "":
                    break
    return title


def _get_manifest_creator(manifest_json):
    """ Get Creator from Manifest JSON [metadata][Author] """
    creator = ""
    if 'metadata' in manifest_json:
        for metadata_item in manifest_json['metadata']:
            if metadata_item['label'].upper() == 'Creator'.upper():
                creator = metadata_item['value']
            if metadata_item['label'].upper() == 'Author'.upper():
                creator = metadata_item['value']
            if metadata_item['label'].upper() == 'Artist'.upper():
                creator = metadata_item['value']
            if creator != "":
                break
    return creator


def _get_manifest_description(manifest_json):
    """ Get Description from Manifest JSON [metadata][Creator] """
    description = ""
    if 'description' in manifest_json:
        description = manifest_json['description']
    return description


def _get_manifest_classification(manifest_json):
    """ Get Classification from Manifest JSON [metadata][Creator] """
    classification = ""
    if 'metadata' in manifest_json:
        for metadata_item in manifest_json['metadata']:
            if metadata_item['label'].upper() == 'classification'.upper():
                classification = metadata_item['value']
            if classification != "":
                break
    if classification == "":
        classification = 'Book'
    return classification


def _build_item_manifest_url_given_id(id):
    return 'https://presentation-iiif.library.nd.edu/' + id + '/manifest'


def _build_collection_manifest_url_given_id(id):
    return 'https://presentation-iiif.library.nd.edu/collection/' + id


def get_manifest_given_url(manifest_url):
    """ Return manifest from URL."""
    manifest_json = {}
    try:
        manifest_json = json.load(request.urlopen(manifest_url))
        # thumbnail = manifest['thumbnail']['@id']
    except error.HTTPError:
        print('Unable to retrieve manifest from ' + manifest_url)
        pass  # If we get a url error, we can't get a thumbnail
    return manifest_json


def _read_s3_file_content(s3Bucket, s3Path):
    content_object = boto3.resource('s3').Object(s3Bucket, s3Path)
    return content_object.get()['Body'].read().decode('utf-8')


# def get_thumbnail_from_manifest(unique_id):
#     """ Open manifest from URL given id.  Retrieve thumbnail from manifest."""
#     thumbnail = ""
#     # manifest_baseurl = 'https://d1v1nx8kcr1acm.cloudfront.net/'
#     manifest_baseurl = 'https://presentation-iiif.library.nd.edu/'
#     try:
#         manifest_url = manifest_baseurl + unique_id + '/manifest/index.json'
#         manifest = json.load(request.urlopen(manifest_url))
#         thumbnail = manifest['thumbnail']['@id']
#     except error.HTTPError:
#         print('Unable to retrieve thumbnail from ' + manifest_url + ' by passing unique_id: ' + unique_id)
#         pass  # If we get a url error, we can't get a thumbnail
#     return thumbnail

# test
# python -c "from get_manifest_info import get_manifest_info;  get_manifest_info('abel-blanchard-correspondence')"
