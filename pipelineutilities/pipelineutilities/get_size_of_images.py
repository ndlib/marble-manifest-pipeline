""" Get Size of Images"""

import json
import requests
from sentry_sdk import capture_exception


def get_size_of_images(standard_json: dict) -> dict:
    for item in standard_json.get('items', {}):
        if item.get('level', '') == 'file':
            uri = item.get('iiifImageUri', '')
            if uri:
                size_dict = _get_image_size_given_uri(uri)
                item.update(size_dict)
        else:
            item = get_size_of_images(item)
    return standard_json


def _get_image_size_given_uri(uri: str) -> dict:
    """ Get image width and height from image server """
    size_dict = {}
    if uri:
        if '/info.json' not in uri:
            uri += '/info.json'
        info_json = _get_json_given_url(uri)
        if 'width' in info_json:
            size_dict['width'] = info_json['width']
        if 'height' in info_json:
            size_dict['height'] = info_json['height']
    return size_dict


def _get_json_given_url(url: str) -> dict:
    """ Return json from URL."""
    json_response = {}
    try:
        json_response = json.loads(requests.get(url).text)
    except ConnectionRefusedError as e:
        print('Connection refused on url ', url)
        capture_exception(e)
    except TimeoutError as e:
        capture_exception(e)
        print("TimeoutError calling " + url)
    except Exception as e:  # noqa E722 - intentionally ignore warning about bare except
        # Presumably, url does not exist.  Do not throw error
        pass
    return json_response
