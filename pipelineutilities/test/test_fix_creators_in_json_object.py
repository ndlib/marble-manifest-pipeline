# test_fix_creators_in_json_object.py
""" test fix_creators_in_json_object """
import _set_path  # noqa
import os
import unittest
from pipelineutilities.fix_creators_in_json_object import FixCreatorsInJsonObject


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        config = {}
        self.fix_creators_in_json_object_class = FixCreatorsInJsonObject(config)

    def test_01_test_known_creators_exist(self):
        """ test 1 test_known_creators_exist """
        standard_json = {
            "creators": [
                {
                    "fullName": "Bunting, Edward.",
                    "display": "Bunting, Edward."
                }
            ]
        }
        results = self.fix_creators_in_json_object_class.known_creators_exist(standard_json)
        self.assertTrue(results)

    def test_02_test_known_creators_exist(self):
        """ test 2 test_known_creators_exist """
        standard_json = {
            "creators": [
                {
                    "fullName": "unknown",
                    "display": "unknown"
                }
            ]
        }
        results = self.fix_creators_in_json_object_class.known_creators_exist(standard_json)
        self.assertFalse(results)

    def test_03_test_known_creators_exist(self):
        """ test 3 test_known_creators_exist """
        standard_json = {"creators": []}
        results = self.fix_creators_in_json_object_class.known_creators_exist(standard_json)
        self.assertFalse(results)

    def test_04_test_known_creators_exist(self):
        """ test 4 test_known_creators_exist """
        standard_json = {
            "creators": [
                {
                    "fullName": "Bunting, Edward.",
                    "display": "Bunting, Edward."
                },
                {
                    "fullName": "unknown",
                    "display": "unknown"
                }
            ]
        }
        results = self.fix_creators_in_json_object_class.known_creators_exist(standard_json)
        self.assertTrue(results)

    def test_05_test_contributors_exist(self):
        """ test 5 contributors_exist """
        standard_json = {
            "contributors": [
                {
                    "fullName": "Bunting, Edward, 1773-1843",
                    "lifeDates": "1773-1843",
                    "display": "Bunting, Edward, 1773-1843"
                }
            ]
        }
        results = self.fix_creators_in_json_object_class.contributors_exist(standard_json)
        self.assertTrue(results)

    def test_06_test_contributors_exist(self):
        """ test 6 contributors_exist """
        standard_json = {"contributors": []}
        results = self.fix_creators_in_json_object_class.contributors_exist(standard_json)
        self.assertFalse(results)

    def test_07_test_unknown_creators_exist(self):
        """ test 7 test_unknown_creators_exist """
        standard_json = {
            "creators": [
                {
                    "fullName": "Bunting, Edward.",
                    "display": "Bunting, Edward."
                }
            ]
        }
        results = self.fix_creators_in_json_object_class.unknown_creators_exist(standard_json)
        self.assertFalse(results)

    def test_08_test_unknown_creators_exist(self):
        """ test 8 test_unknown_creators_exist """
        standard_json = {
            "creators": [
                {
                    "fullName": "Bunting, Edward.",
                    "display": "Bunting, Edward."
                },
                {
                    "fullName": "unknown",
                    "display": "unknown"
                }
            ]
        }
        results = self.fix_creators_in_json_object_class.unknown_creators_exist(standard_json)
        self.assertTrue(results)

    def test_09_test_remove_unknown_creator(self):
        """ test 9 test_remove_unknown_creator """
        standard_json = {
            "creators": [
                {
                    "fullName": "unknown",
                    "display": "unknown"
                },
                {
                    "fullName": "Bunting, Edward.",
                    "display": "Bunting, Edward."
                },
                {
                    "fullName": "unknown",
                    "display": "unknown"
                }
            ]
        }
        actual_results = self.fix_creators_in_json_object_class.remove_unknown_creators(standard_json)
        expected_results = {
            "creators": [
                {
                    "fullName": "Bunting, Edward.",
                    "display": "Bunting, Edward."
                }
            ]
        }
        self.assertEqual(actual_results, expected_results)

    def test_10_test_add_unknown_creator(self):
        """ test 10 add_unknown_creator """
        standard_json = {"sourceSystem": "Aleph"}
        actual_results = self.fix_creators_in_json_object_class.add_unknown_creators(standard_json)
        expected_results = {
            "sourceSystem": "Aleph",
            "creators": [
                {
                    "fullName": "unknown",
                    "display": "unknown"
                }
            ]
        }
        self.assertEqual(actual_results, expected_results)

    def test_11_test_add_unknown_creator(self):
        """ test 11 add_unknown_creator """
        standard_json = {"sourceSystem": "Aleph", "creators": []}
        actual_results = self.fix_creators_in_json_object_class.add_unknown_creators(standard_json)
        expected_results = {
            "sourceSystem": "Aleph",
            "creators": [
                {
                    "fullName": "unknown",
                    "display": "unknown"
                }
            ]
        }
        self.assertEqual(actual_results, expected_results)

    def test_11a_test_add_unknown_creator(self):
        """ test 11a add_unknown_creator for non-Aleph sourceSystem (If not Aleph, return what was passed) """
        standard_json = {"creators": []}
        actual_results = self.fix_creators_in_json_object_class.add_unknown_creators(standard_json)
        expected_results = {"creators": []}
        self.assertEqual(actual_results, expected_results)

    def test_12_test_remove_unknown_creators_if_known_creators_exist(self):
        """ test 12 test_remove_unknown_creators_if_known_creators_exist """
        standard_json = {
            "creators": [
                {
                    "fullName": "unknown",
                    "display": "unknown"
                },
                {
                    "fullName": "Bunting, Edward.",
                    "display": "Bunting, Edward."
                },
                {
                    "fullName": "unknown",
                    "display": "unknown"
                }
            ]
        }
        actual_results = self.fix_creators_in_json_object_class.remove_unknown_creators_if_known_creators_exist(standard_json)
        expected_results = {
            "creators": [
                {
                    "fullName": "Bunting, Edward.",
                    "display": "Bunting, Edward."
                }
            ]
        }
        self.assertEqual(actual_results, expected_results)

    def test_13_test_fix_creators_for_manifest(self):
        """ test 13 test_fix_creators_for_manifest """
        standard_json = {
            "contributors": [
                {
                    "fullName": "Bunting, Edward.",
                    "display": "Bunting, Edward."
                },
            ],
            "creators": [
                {
                    "fullName": "unknown",
                    "display": "unknown"
                }
            ]
        }
        actual_results = self.fix_creators_in_json_object_class.fix_creators_for_manifest(standard_json)
        expected_results = {
            "contributors": [
                {
                    "fullName": "Bunting, Edward.",
                    "display": "Bunting, Edward."
                }
            ],
            "creators": []
        }
        self.assertEqual(actual_results, expected_results)

    def test_14_test_fix_creators_for_manifest(self):
        """ test 14 test_fix_creators_for_manifest """
        standard_json = {"sourceSystem": "Aleph"}
        actual_results = self.fix_creators_in_json_object_class.fix_creators_for_manifest(standard_json)
        expected_results = {
            "sourceSystem": "Aleph",
            "creators": [
                {
                    "fullName": "unknown",
                    "display": "unknown"
                }
            ]
        }
        self.assertEqual(actual_results, expected_results)

    def test_15_test_fix_creators_for_manifest(self):
        """ test 15 test_fix_creators_for_manifest """
        standard_json = {
            "contributors": [
                {
                    "fullName": "Bunting, Edward.",
                    "display": "Bunting, Edward."
                },
            ]
        }
        actual_results = self.fix_creators_in_json_object_class.fix_creators_for_manifest(standard_json)
        expected_results = {
            "contributors": [
                {
                    "fullName": "Bunting, Edward.",
                    "display": "Bunting, Edward."
                }
            ]
        }
        self.assertEqual(actual_results, expected_results)

    def test_16_test_fix_creators_for_collection(self):
        """ test 16 test_fix_creators_for_collection """
        standard_json = {
            "creators": [
                {
                    "fullName": "unknown",
                    "display": "unknown"
                },
            ]
        }
        actual_results = self.fix_creators_in_json_object_class.fix_creators_for_collection(standard_json)
        expected_results = {"creators": []}
        self.assertEqual(actual_results, expected_results)

    def test_17_test_fix_creators(self):
        """ test 17 test_fix_creators """
        standard_json = {
            "id": 1,
            "level": "collection",
            "creators": [
                {
                    "fullName": "unknown",
                    "display": "unknown"
                },
            ],
            "items": [
                {"id": 2, "level": "manifest"},
                {"id": 3, "level": "manifest", "creators": [{"fullName": "unknown"}], "contributors": [{"fullName": "someone"}]}
            ]
        }
        actual_results = self.fix_creators_in_json_object_class.fix_creators(standard_json)
        expected_results = {
            "id": 1,
            "level": "collection",
            "items": [
                {"id": 2, "level": "manifest"},
                {"id": 3, "level": "manifest", "contributors": [{"fullName": "someone"}]}
            ]
        }
        self.assertEqual(actual_results, expected_results)

    def test_18_test_fix_creators(self):
        """ test 18 test_fix_creators """
        standard_json = {
            "id": "1",
            "creators": [
                {
                    "attribution": "",
                    "role": "Primary",
                    "fullName": "Sorin, Edward",
                    "display": "Sorin, Edward"
                },
                {
                    "fullName": "unknown",
                    "display": "unknown"
                }
            ],
            "level": "manifest"
        }
        actual_results = self.fix_creators_in_json_object_class.fix_creators(standard_json)
        expected_results = {
            "id": "1",
            "creators": [
                {
                    "attribution": "",
                    "role": "Primary",
                    "fullName": "Sorin, Edward",
                    "display": "Sorin, Edward"
                }],
            "level": "manifest"
        }
        # print("actual_results = ", actual_results)
        self.assertEqual(actual_results, expected_results)

    def test_19_remove_unknown_creators(self):
        """ test 19 remove_unknown_creators """
        standard_json = {
            "id": "1",
            "creators": [
                {
                    "attribution": "",
                    "role": "Primary",
                    "fullName": "Sorin, Edward",
                    "display": "Sorin, Edward"
                },
                {
                    "fullName": "unknown",
                    "display": "unknown"
                }
            ],
            "level": "manifest"
        }
        actual_results = self.fix_creators_in_json_object_class.remove_unknown_creators(standard_json)
        expected_results = {
            "id": "1",
            "creators": [
                {
                    "attribution": "",
                    "role": "Primary",
                    "fullName": "Sorin, Edward",
                    "display": "Sorin, Edward"
                }],
            "level": "manifest"
        }
        # print("actual_results = ", actual_results)
        self.assertEqual(actual_results, expected_results)

    def test_20_remove_unknown_creators_if_known_creators_exist(self):
        """ test 20 remove_unknown_creators_if_known_creators_exist """
        standard_json = {
            "id": "1",
            "creators": [
                {
                    "attribution": "",
                    "role": "Primary",
                    "fullName": "Sorin, Edward",
                    "display": "Sorin, Edward"
                },
                {
                    "fullName": "unknown",
                    "display": "unknown"
                }
            ],
            "level": "manifest"
        }
        actual_results = self.fix_creators_in_json_object_class.remove_unknown_creators_if_known_creators_exist(standard_json)
        expected_results = {
            "id": "1",
            "creators": [
                {
                    "attribution": "",
                    "role": "Primary",
                    "fullName": "Sorin, Edward",
                    "display": "Sorin, Edward"
                }],
            "level": "manifest"
        }
        # print("actual_results = ", actual_results)
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
