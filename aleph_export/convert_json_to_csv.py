# convert_json_to_csv.py
import json
import copy
from dependencies.pipelineutilities.output_csv import OutputCsv


class ConvertJsonToCsv():
    def __init__(self, csv_field_names: dict):
        self.output_csv_class = OutputCsv(csv_field_names)

    def convert_json_to_csv(self, json_object: dict) -> str:
        """ Save this json object as a csv, then return the csv string """
        local_copy_of_json = copy.deepcopy(json_object)
        self._save_json_record_to_csv(local_copy_of_json)
        return self.output_csv_class.return_csv_value()

    def _save_json_record_to_csv(self, json_object: dict):
        """ Recursively save this json_object as a csv string """
        self.output_csv_class.write_csv_row(json_object)
        if "items" in json_object:
            if "[" in json_object["items"] and "]" in json_object["items"]:
                json_object["items"] = json.loads(json_object["items"])
            for item in json_object["items"]:
                self._save_json_record_to_csv(item)
