from dependencies.pipelineutilities.output_csv import OutputCsv
import json


class CsvFromJson():
    """ This performs all Marc-related processing """
    def __init__(self, csv_field_names):
        """ Save values required for all calls """
        self.output_csv_class = OutputCsv(csv_field_names)

    def return_csv_from_json(self, json_record):
        """ Given marc_record_as_json, create the csv describing that object, including related file information. """
        if json_record:
            self.output_csv_class.write_csv_header
            self._recursively_write_csv_from_json_tree(json_record)
            csv_string = self.output_csv_class.return_csv_value()
        return csv_string

    def _recursively_write_csv_from_json_tree(self, json_node):
        """ Do a depth first traversal of our tree, writing a row of CSV for each node """
        if json_node:
            self.output_csv_class.write_csv_row(json_node)
            if "children" in json_node:
                if isinstance(json_node["children"], str):
                    json_node["children"] = json.loads(json_node["children"])  # for some reason, this is a list of strings instead of dictionaries.
                for child in json_node["children"]:
                    if isinstance(child, dict):
                        self._recursively_write_csv_from_json_tree(child)
        return
