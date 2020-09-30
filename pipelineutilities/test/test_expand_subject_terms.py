""" test_expand_subject_terms """
import _set_path  # noqa: F401
import json
import os
from pipelineutilities.expand_subject_terms import _add_display_to_broader_terms
import unittest


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"

with open(local_folder + 'loc_term_85012415.json', 'r', encoding='utf-8') as json_file:
    loc_term_85012315 = json.load(json_file)


class Test(unittest.TestCase):
    """ Test expand_getty_aat_terms """

    def test_01_add_display_to_broader_terms(self):
        """ test_01_add_display_to_broader_terms """
        sample_subject = {
            "term": "Bryology.",
            "uri": "http://id.loc.gov/authorities/subjects/sh85017378",
            "authority": "LCSH",
            "variants": [
                "Muscology"
            ],
            "broaderTerms": [
                {
                    "uri": "http://id.loc.gov/authorities/subjects/sh85015976",
                    "authority": "LCSH",
                    "term": "Botany",
                    "variants": [
                        "Botanical science",
                        "Phytobiology",
                        "Phytography",
                        "Phytology",
                        "Plant biology",
                        "Plant science"
                    ]
                }
            ]
        }
        actual_results = _add_display_to_broader_terms(sample_subject)
        expected_results = {
            "term": "Bryology.",
            "uri": "http://id.loc.gov/authorities/subjects/sh85017378",
            "authority": "LCSH",
            "variants": [
                "Muscology"
            ],
            "broaderTerms": [
                {
                    "display": "Botany",
                    "uri": "http://id.loc.gov/authorities/subjects/sh85015976",
                    "authority": "LCSH",
                    "term": "Botany",
                    "variants": [
                        "Botanical science",
                        "Phytobiology",
                        "Phytography",
                        "Phytology",
                        "Plant biology",
                        "Plant science"
                    ]
                }
            ]
        }
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
