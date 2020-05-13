# add_paths_to_nd_json.py
from pipelineutilities.csv_collection import _add_additional_paths


class AddPathsToNdJson():
    """ This class accepts a JSON object and adds paths necessary for creating manifests. """
    def __init__(self, config: dict):
        self.config = config

    def add_paths(self, object: dict) -> dict:
        """ This calls all other modules locally """
        _add_additional_paths(object, self.config)
        if "items" in object:
            for item in object["items"]:
                self.add_paths(item)
        return object
