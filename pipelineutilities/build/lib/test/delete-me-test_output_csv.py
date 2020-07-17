import _set_path  # noqa
import unittest
from pipelineutilities.output_csv import OutputCsv  # noqa: E402


class TestOutputCSV(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        self.csv_field_names = ['fieldstr', 'fieldint', 'fieldjson', 'listjson']
        self.output_csv_class = OutputCsv(self.csv_field_names)

    def test_output_csv(self):
        """ Test each datatype to verify csv saves correctly """
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
        self.output_csv_class.write_csv_header()  # clear header in case other tests have run
        self.output_csv_class.write_csv_row(data)
        return_value = self.output_csv_class.return_csv_value()
        expected_value = '"fieldstr","fieldint","fieldjson","listjson"\r\n"str","4","{""json1"": ""json"", ""json2"": ""more json""}","[""list1"", ""list2""]"\r\n'  # noqa # E501
        self.assertEqual(return_value, expected_value)


if __name__ == '__main__':
    unittest.main()
