# clean_up_standard_json.py

import os
from do_extra_processing import define_manifest_level
from pipelineutilities.add_paths_to_json_object import AddPathsToJsonObject
from pipelineutilities.fix_creators_in_json_object import FixCreatorsInJsonObject
from pipelineutilities.validate_json import validate_standard_json


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


def clean_up_standard_json(standard_json: dict, config: dict) -> dict:
    """ If extra processing is required, make appropriate calls to perform that additional processing. """
    standard_json = _fix_level(standard_json)
    standard_json = _fix_ids(standard_json)
    add_paths_to_json_object_class = AddPathsToJsonObject(config)
    fix_creators_in_json_object_class = FixCreatorsInJsonObject(config)
    standard_json = add_paths_to_json_object_class.add_paths(standard_json)
    standard_json = fix_creators_in_json_object_class.fix_creators(standard_json)
    if not validate_standard_json(standard_json):
        standard_json = {}
    return standard_json


def _fix_level(standard_json: dict) -> dict:
    """ If level is not defined as "file", fix level based on rules elsewhere  """
    level = "manifest"
    if "items" in standard_json:
        if len(standard_json["items"]) > 0:
            level = define_manifest_level(standard_json["items"])
            standard_json["level"] = level
            for item in standard_json["items"]:
                if item.get("level", "") != "file":
                    item = _fix_level(item)
    return standard_json


def _fix_ids(standard_json: dict, collection_id: str = None, parent_id: str = None) -> dict:
    """ Because of the way we append "members" to the json tree, we need to fix collectionId and parentId """
    if collection_id is None:
        collection_id = standard_json.get("collectionId", standard_json.get("id", ""))
    standard_json["collectionId"] = collection_id
    if parent_id is not None:
        standard_json["parentId"] = parent_id
    if "items" in standard_json:
        parent_id = standard_json.get("id", "")
        for item in standard_json["items"]:
            item = _fix_ids(item, collection_id, parent_id)
    return standard_json
