import csv
import io
import json


class OutputCsv():
    """ This performs all CSV-related processing """

    def __init__(self, csv_field_names):
        """ Define things required for subsequent processing """
        self.csv_string_io = io.StringIO()
        self.csv_writer = csv.DictWriter(self.csv_string_io,
                                         fieldnames=csv_field_names,
                                         extrasaction='ignore',
                                         quoting=csv.QUOTE_ALL)
        self.write_csv_header()

    def write_csv_header(self):
        """ Write header fields to csv string io """
        self.csv_string_io.truncate(0)  # truncate anything in the io buffer
        self.csv_string_io.seek(0)  # reposition the pointer to the beginning of the buffer
        self.csv_writer.writeheader()

    def write_csv_row(self, json_node):
        """ Write one row to the csv string io """
        for key in json_node.keys():
            if type(json_node[key]) is dict or type(json_node[key]) is list:
                json_node[key] = json.dumps(json_node[key])

        self.csv_writer.writerow(json_node)

    def return_csv_value(self):
        """ Return contents of csv as a string """
        return self.csv_string_io.getvalue()


# python -c 'from output_csv import *; test()'
def test():
    csv = OutputCsv(['fieldstr', 'fieldint', 'fieldjson', 'listjson'])
    data = {
        "fieldstr": "str",
        "fieldint": 4,
        "fieldjson": {
            "json1": "json",
            "json2": "more json"
        },
        "listjson": [
            "list1", "list2"
        ]
    }
    csv.write_csv_row(data)
    print(csv.return_csv_value())
