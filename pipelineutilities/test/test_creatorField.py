import unittest
import os
import sys
where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am + "/../")
from pipelineutilities.creatorField import creatorField   # noqa: E402


class TestCreatorField(unittest.TestCase):
    def setUp(self):
        pass

    def test_one_fullname(self):
        data = [{'fullName': "fullname"}]
        result = ["fullname"]
        c = creatorField(data)
        for index, creator in enumerate(c.to_array()):
            self.assertEqual(result[index], creator)

    def test_one_nationality(self):
        data = [{'fullName': "fullname", 'nationality': 'mars'}]
        result = ["fullname (mars)"]
        c = creatorField(data)
        for index, creator in enumerate(c.to_array()):
            self.assertEqual(result[index], creator)

    def test_one_lifedates(self):
        data = [{'fullName': "fullname", 'lifeDates': '1900-2000'}]
        result = ["fullname (1900-2000)"]
        c = creatorField(data)
        for index, creator in enumerate(c.to_array()):
            self.assertEqual(result[index], creator)

    def test_one_both_nationality_and_lifedates(self):
        data = [{'fullName': "fullname", 'nationality': 'mars', 'lifeDates': '1900-2000'}]
        result = ["fullname (mars, 1900-2000)"]
        c = creatorField(data)
        for index, creator in enumerate(c.to_array()):
            self.assertEqual(result[index], creator)

    def test_one_attribution(self):
        data = [{'attribution': 'attr', 'fullName': "fullname"}]
        result = ["attr fullname"]
        c = creatorField(data)
        for index, creator in enumerate(c.to_array()):
            self.assertEqual(result[index], creator)

    def test_one_full(self):
        data = [{'attribution': 'attr', 'fullName': "fullname", 'nationality': 'mars', 'lifeDates': '1900-2000'}]
        result = ["attr fullname (mars, 1900-2000)"]
        c = creatorField(data)
        for index, creator in enumerate(c.to_array()):
            self.assertEqual(result[index], creator)

    def test_sorts_primary_to_the_top(self):
        data = [
            {'role': 'Secondary', 'fullName': "secondary"},
            {'role': 'Primary', 'fullName': "primary"},
        ]
        result = ["primary", 'secondary']
        c = creatorField(data)
        for index, creator in enumerate(c.to_array()):
            self.assertEqual(result[index], creator)

    def test_multiple_creators(self):
        data = [
            {'role': 'Primary', 'fullName': "primary", 'nationality': 'mars'},
            {'role': 'Secondary', 'fullName': "secondary", 'attribution': 'attr'},
            {'role': 'Secondary', 'fullName': "third", 'nationality': 'mars', 'lifeDates': '1900-2000'},
        ]
        result = ["primary (mars)", 'attr secondary', 'third (mars, 1900-2000)']
        c = creatorField(data)
        for index, creator in enumerate(c.to_array()):
            self.assertEqual(result[index], creator)

    def test__make_lifeDate(self):
        data = [
            {"lifeDates": "something"},
            {"lifeDates": "something", "livingFlag": "0", "startDate": "1234", "endDate": "1289"},
            {"lifeDates": "something", "livingFlag": "0", "startDate": "1234"},
            {"lifeDates": "something", "livingFlag": "0", "startDate": "1962", "endDate": "2020"},
            {"lifeDates": "something", "livingFlag": "1", "startDate": "1962", "endDate": "2020"},
            {"lifeDates": "something", "livingFlag": "1", "startDate": "1962"},
            {"livingFlag": "1", "startDate": "1962", "endDate": "2020"},
            {"livingFlag": "1", "startDate": "1962"},
        ]
        result = ["something", "1234 - 1289", "1234 - ", "1962 - 2020", "1962 - 2020", "1962", "1962 - 2020", "1962"]
        c = creatorField(data)
        for index, creator in enumerate(data):
            self.assertEqual(result[index], c._make_lifeDate(creator))

    def test__update_lifeDate(self):
        data = [
            {"lifeDates": "something"},
            {"lifeDates": "something", "livingFlag": "0", "startDate": "1234", "endDate": "1289"},
            {"lifeDates": "something", "livingFlag": "0", "startDate": "1234"},
            {"lifeDates": "something", "livingFlag": "0", "startDate": "1962", "endDate": "2020"},
            {"lifeDates": "something", "livingFlag": "1", "startDate": "1962", "endDate": "2020"},
            {"lifeDates": "something", "livingFlag": "1", "startDate": "1962"},
            {"livingFlag": "1", "startDate": "1962", "endDate": "2020"},
            {"livingFlag": "1", "startDate": "1962"},
        ]
        result = ["something", "1234 - 1289", "1234 - ", "1962 - 2020", "1962 - 2020", "1962", "1962 - 2020", "1962"]
        c = creatorField(data)
        for index, creator in enumerate(data):
            c._update_lifeDate(creator)
            self.assertEqual(result[index], creator["lifeDates"])

    def test__b_for_person_birthdate(self):
        data = [
            {},
            {"lifeFlag": "0"},
            {"lifeFlag": "1", "livingFlag": "0"},
            {"lifeFlag": "1", "livingFlag": "1"},
            {"lifeFlag": "1", "livingFlag": "1", "endDate": "2020"},
        ]
        result = ["", "", "", "b. ", ""]
        c = creatorField(data)
        for index, creator in enumerate(data):
            self.assertEqual(result[index], c._b_for_person_birthdate(creator))

    def test__active_for_corporate_creator(self):
        data = [
            {},
            {"lifeFlag": "0"},
            {"lifeFlag": "1"},
        ]
        result = ["", "active ", ""]
        c = creatorField(data)
        for index, creator in enumerate(data):
            self.assertEqual(result[index], c._active_for_corporate_creator(creator))

    def test__make_display(self):
        data = [
            {'role': 'Primary', 'fullName': "primary", 'nationality': 'mars'},
            {'role': 'Secondary', 'fullName': "secondary", 'attribution': 'attr'},
            {'role': 'Secondary', 'fullName': "third", 'nationality': 'mars', 'lifeDates': '1900-2000'},
            {"fullName": "test person"},
            {"fullName": "5 person", "lifeFlag": "1", "livingFlag": "1", "startDate": "1900"},
            {"fullName": "6 corporate", "lifeFlag": "0", "livingFlag": "1", "startDate": "1900"},
            {"fullName": "7 corporate", "lifeFlag": "0", "livingFlag": "1", "startDate": "1900", "endDate": "present"},
            {"fullName": "8 ambiguous living", "lifeFlag": "1", "livingFlag": "1", "startDate": "1962", "endDate": "2020"},
            {"fullName": "9 dead", "lifeFlag": "1", "livingFlag": "0", "startDate": "1962", "endDate": "2020"},
            {"fullName": "10 living", "lifeFlag": "1", "livingFlag": "1", "startDate": "1962"},
        ]
        result = ["primary (mars)", 'attr secondary', 'third (mars, 1900-2000)', "test person",
                  "5 person (b. 1900)", "6 corporate (active 1900)", "7 corporate (active 1900 - present)",
                  "8 ambiguous living (1962 - 2020)", "9 dead (1962 - 2020)", "10 living (b. 1962)"
                  ]
        c = creatorField(data)
        for index, creator in enumerate(data):
            c._update_lifeDate(creator)
            self.assertEqual(result[index], c.make_string(creator))

    def test_add_display(self):
        """ Note that these intentionally will not sort to change the order for testing. """
        data = [
            {"fullName": "test"},
            {"fullName": "5 person", "lifeFlag": "1", "livingFlag": "1", "startDate": "1900"},
            {"fullName": "6 corporate", "lifeFlag": "0", "livingFlag": "1", "startDate": "1900"},
            {"fullName": "7 corporate", "lifeFlag": "0", "livingFlag": "1", "startDate": "1900", "endDate": "present"},
            {"fullName": "8 ambiguous living", "lifeFlag": "1", "livingFlag": "1", "startDate": "1962", "endDate": "2020"},
            {"fullName": "9 dead", "lifeFlag": "1", "livingFlag": "0", "startDate": "1962", "endDate": "2020"},
            {"fullName": "10 living", "lifeFlag": "1", "livingFlag": "1", "startDate": "1962"},
        ]
        result = ["test", "5 person (b. 1900)", "6 corporate (active 1900)", "7 corporate (active 1900 - present)",
                  "8 ambiguous living (1962 - 2020)", "9 dead (1962 - 2020)", "10 living (b. 1962)"
                  ]
        c = creatorField(data)
        data = c.add_displays()
        for index, creator in enumerate(data):
            self.assertEqual(result[index], creator["display"])


if __name__ == '__main__':
    unittest.main()
