""" test_expand_getty_ia_terms """
import _set_path  # noqa
import json
import os
from pipelineutilities.expand_loc_terms import _get_api_url, _get_str_value, _get_loc_item_node, _get_loc_variants, \
    expand_loc_terms
import unittest
from unittest.mock import patch


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"

with open(local_folder + 'loc_term_85012415.json', 'r', encoding='utf-8') as json_file:
    loc_term_85012315 = json.load(json_file)


class Test(unittest.TestCase):
    """ Test expand_getty_aat_terms """

    def test_01_get_api_url(self):
        """ test_01_get_api_url """
        human_url = "https://id.loc.gov/authorities/subjects/sh85012415.html"
        actual_results = _get_api_url(human_url)
        expected_results = "http://id.loc.gov/authorities/subjects/sh85012415"
        self.assertEqual(actual_results, expected_results)

    def test_02_get_str_value(self):
        """ test_02_get_str_value """
        sample_json = {
            "@id": "_:b17iddOtlocdOtgovauthoritiessubjectssh85012415",
            "@type": [
                "http://www.loc.gov/mads/rdf/v1#TopicElement"
            ],
            "http://www.loc.gov/mads/rdf/v1#elementValue": [
                {
                    "@language": "en",
                    "@value": "Hitting (Baseball)"
                }
            ]
        }
        actual_results = _get_str_value(sample_json, "http://www.loc.gov/mads/rdf/v1#elementValue")
        expected_results = "Hitting (Baseball)"
        self.assertEqual(actual_results, expected_results)
        sample_json = {
            "@id": "http://id.loc.gov/authorities/subjects/sh85012415",
            "@type": [
                "http://www.loc.gov/mads/rdf/v1#Topic",
                "http://www.loc.gov/mads/rdf/v1#Authority",
                "http://www.w3.org/2004/02/skos/core#Concept"
            ],
            "http://www.loc.gov/mads/rdf/v1#authoritativeLabel": [
                {
                    "@value": "Batting (Baseball)"
                }
            ],
        }
        actual_results = _get_str_value(sample_json, "http://www.loc.gov/mads/rdf/v1#authoritativeLabel")
        expected_results = "Batting (Baseball)"
        self.assertEqual(actual_results, expected_results)

    def test_03_get_loc_item_node(self):
        """ test_03_get_loc_item_node """
        actual_results = _get_loc_item_node(loc_term_85012315, "http://id.loc.gov/resources/works/1444948")
        expected_results = {
            "@id": "http://id.loc.gov/resources/works/1444948",
            "@type": [
                "http://id.loc.gov/ontologies/bibframe/Work"
            ],
            "http://id.loc.gov/ontologies/bflc/aap": [
                {
                    "@value": "Macaulay, Tom, 1947- How to hit .400 : the physical and mental fundamentals of hitting a baseball"
                }
            ]
        }
        self.assertEqual(actual_results, expected_results)

    def test_04_get_loc_variants(self):
        """ test_04_get_loc_item_node """
        api_url = "http://id.loc.gov/authorities/subjects/sh85012415"
        loc_item_node = _get_loc_item_node(loc_term_85012315, api_url)
        variant_links = loc_item_node.get("http://www.loc.gov/mads/rdf/v1#hasVariant", [])
        actual_results = _get_loc_variants(loc_term_85012315, variant_links)
        expected_results = ["Hitting (Baseball)"]
        self.assertEqual(actual_results, expected_results)

    def test_05_get_preferred_term(self):
        """ test_05_get_preferred_term """
        api_url = "http://id.loc.gov/authorities/subjects/sh85012415"
        loc_item_node = _get_loc_item_node(loc_term_85012315, api_url)
        actual_results = _get_str_value(loc_item_node, 'http://www.loc.gov/mads/rdf/v1#authoritativeLabel')
        expected_results = "Batting (Baseball)"
        self.assertEqual(actual_results, expected_results)

    @patch('pipelineutilities.expand_loc_terms._get_json_given_url')
    def test_02_get_broader_terms_given_id(self, mock_get_json_given_url):
        """ verify _get_broader terms_given_id """
        mock_get_json_given_url.return_value = loc_term_85012315
        sample_subject = {
            "uri": "http://id.loc.gov/authorities/subjects/sh85012415",
        }
        actual_results = expand_loc_terms(sample_subject, 2)
        expected_results = {
            "term": "Batting (Baseball)",
            "uri": "http://id.loc.gov/authorities/subjects/sh85012415",
            "authority": "LCSH",
            "variants": ["Hitting (Baseball)"],
        }
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
