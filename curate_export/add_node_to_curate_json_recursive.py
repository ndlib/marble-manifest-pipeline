""" This is a single-use module to add a node to the Curate lantern slides export (which is huge).
Even though this is intended to be for a single use, I'll keep here for now for future reference. """

import _set_path  # noqa
import os
import io
import json


def _add_node_to_curate_json_recursive(curate_json: dict, node_name: str, node_value: str) -> dict:
    curate_json[node_name] = node_value
    for member in curate_json.get('members', []):
        for _k, v in member.items():
            if isinstance(v, dict):
                _add_node_to_curate_json_recursive(v, node_name, node_value)
                for contained_file in v.get('containedFiles', []):
                    _add_node_to_curate_json_recursive(contained_file, node_name, node_value)
    for contained_file in curate_json.get('containedFiles', []):
        _add_node_to_curate_json_recursive(contained_file, node_name, node_value)


# python -c 'from add_node_to_curate_json_recursive import *; test()'
def test(identifier=""):
    """ test exection """
    filename = './save/qz20sq9094h_curate_original.json'
    if os.path.exists(filename):
        with io.open(filename, 'r', encoding='utf-8') as json_file:
            curate_json = json.load(json_file)
        _add_node_to_curate_json_recursive(curate_json, "permissions#use", "To view the physical lantern slide, please contact the Architecture Library to arrange an appointment.")
        filename = './save/qz20sq9094h_curate.json'
        with open(filename, 'w') as f:
            json.dump(curate_json, f, indent=2, sort_keys=True)
