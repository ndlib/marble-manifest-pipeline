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
    manifest_info['library_collection_code'] = _get_library_collection_code(manifest_info)
    manifest_info['display_library'] = _get_display_library(manifest_info)
    return manifest_info


def _get_id_given_manifest_url(manifest_url):
    """ Get unique id from manifest @id """
    id = ""
    # URL is expected to be of the form:  "https://presentation-iiif.library.nd.edu/ils-000909884/manifest"
    if manifest_url.startswith('https://presentation-iiif.library.nd.edu/'):
        id = re.sub('https://presentation-iiif.library.nd.edu/', '', manifest_url)  # strip root url
        id = re.sub('/manifest', '', id)  # strip manifest portion of url if it applies
        id = re.sub('collection/', '', id)  # strip collection portion of url if it applies
    return id


def _harvest_important_manifest_info(manifest_json, manifest_info):
    """ Pull important information from manifest to create search index record """
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
    """ Find Repository from Manifest Attribution """
    attribution = ''
    attribution = _get_manifest_attribution(manifest_json).upper()
    if 'Archives'.upper() in attribution:
        repository = 'UNDA'  # Primo expects Repository of UNDA for Archives
    elif 'Rare Books'.upper() in attribution:
        repository = 'SPEC'  # Primo expects Repository of SPEC for RBSC
    elif 'Snite'.upper() in attribution:
        repository = 'SNITE'
    else:
        repository = 'SNITE'
    return repository


def _get_library(manifest_info):
    """ Determine value of PNX Library given repository """
    l_dictionary = {
        'UNDA': 'HESB',
        'SPEC': 'HESB',
        'SNITE': 'Snite'
    }
    library = l_dictionary.get(manifest_info['repository'].upper(), 'Snite')
    return library


def _get_library_collection_code(manifest_info):
    """ Get Primo lsr01 Collection Code given repository """
    lcc_dictionary = {
        'UNDA': 'UNDA ARCHV',
        'SPEC': 'SPEC',
        'SNITE': 'SNITE'
    }
    library_collection_code = lcc_dictionary.get(manifest_info['repository'].upper(), 'SNITE')
    return library_collection_code


def _get_display_library(manifest_info):
    """ Get human-readable library given repository """
    dl_dictionary = {
        'UNDA': 'University Archives',
        'SPEC': 'Rare Books and Special Collections',
        'SNITE': 'Snite Museum of Art'
    }
    display_library = dl_dictionary.get(manifest_info['repository'].upper(), 'Snite')
    return display_library


def _get_manifest_attribution(manifest_json):
    """ Get Attribution from Manifest JSON [metadata][Creator] """
    attribution = ""
    if 'attribution' in manifest_json:
        attribution = manifest_json['attribution']
    return attribution


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
    """ Get Description from Manifest JSON [metadata][description] """
    description = ""
    if 'description' in manifest_json:
        description = manifest_json['description']
    return description


def _get_manifest_classification(manifest_json):
    """ Get Classification from Manifest JSON [metadata][classification] """
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
    """ Build url for Item Manifest given id """
    return 'https://presentation-iiif.library.nd.edu/' + id + '/manifest'


def _build_collection_manifest_url_given_id(id):
    """ Build url for Collection Manifest given id """
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
    """ Read contents of s3 file """
    content_object = boto3.resource('s3').Object(s3Bucket, s3Path)
    return content_object.get()['Body'].read().decode('utf-8')


# test
# python -c "from get_manifest_info import get_manifest_info;  get_manifest_info('abel-blanchard-correspondence')"
