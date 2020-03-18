import unittest
from creatorField import creatorField


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


if __name__ == '__main__':
    unittest.main()
