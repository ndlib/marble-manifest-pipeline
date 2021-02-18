""" test_expand_subject_terms """
import _set_path  # noqa: F401
import json
import os
from pipelineutilities.expand_subject_terms import _add_display_to_broader_terms, _define_subject_authority, _get_offset_days, _get_last_modified_date_to_save
import unittest
from datetime import datetime, timedelta


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

    def test_02_define_subject_authority(self):
        """  test_02_define_subject_authority """
        self.assertEqual('LCSH', _define_subject_authority('LCSH', 'someuri'))
        self.assertEqual('LCSH', _define_subject_authority('LOC', 'someuri'))
        self.assertEqual('LCSH', _define_subject_authority('LIBRARY OF CONGRESS SUBJECT HEADINGS', 'someuri'))
        self.assertEqual('LCSH', _define_subject_authority('xxx', 'www/id.loc.gov/something'))
        self.assertEqual('IA', _define_subject_authority('IA', 'someuri'))
        self.assertEqual('IA', _define_subject_authority('xxx', 'www/vocab.getty.edu/page/ia/something'))
        self.assertEqual('AAT', _define_subject_authority('AAT', 'someuri'))
        self.assertEqual('AAT', _define_subject_authority('xxx', 'www/vocab.getty.edu/aat/something'))
        self.assertEqual('FAST', _define_subject_authority('FAST', 'someuri'))
        self.assertEqual('FAST', _define_subject_authority('xxx', 'www/id.worldcat.org/fast/something'))
        self.assertEqual('LOCAL', _define_subject_authority('LOCAL', 'someuri'))
        self.assertEqual('ANYTHINGELSE', _define_subject_authority('anythingElse', 'someuri'))
        self.assertEqual('', _define_subject_authority('', 'someuri'))

    def test_03_get_offset_days(self):
        """ test_03_get_offset_days """
        self.assertEqual(0, _get_offset_days('2021-02-17'))
        self.assertGreaterEqual(30, _get_offset_days(None))
        self.assertGreaterEqual(30, _get_offset_days(''))
        self.assertLessEqual(1, _get_offset_days(None))
        self.assertLessEqual(1, _get_offset_days(''))

    def test_05_get_last_modified_date_to_save(self):
        """ test_03_get_offset_days
            Note:  Since we're stripping off seconds and microseconds,
                this test should fail only on minute boundaries.
                Since each call takes about 17 microseconds to execute,
                this should only error about 17 times out of 60 million. (60 seconds * 1 million micro seconds)"""
        self.assertEqual(datetime.now().isoformat()[:16], _get_last_modified_date_to_save('2021-02-17')[:16])
        self.assertGreaterEqual(datetime.now().isoformat()[:16], _get_last_modified_date_to_save('2021-02-17')[:16])
        self.assertLessEqual((datetime.now() - timedelta(days=30)).isoformat()[:16], _get_last_modified_date_to_save('2021-02-17')[:16])


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
