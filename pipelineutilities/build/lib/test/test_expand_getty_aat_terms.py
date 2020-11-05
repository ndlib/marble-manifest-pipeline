""" test_expand_getty_aat_terms """
import _set_path  # noqa
import os
from pipelineutilities.expand_getty_aat_terms import expand_aat_terms, _parse_parent_string_into_broader_terms, \
    _get_term_from_string, _get_uri_from_string, _get_api_url
import unittest
from unittest.mock import patch


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Test expand_getty_aat_terms """

    def test_01_get_api_url(self):
        """ test_01_get_api_url """
        human_url = "http://vocab.getty.edu/aat/300046020"
        actual_results = _get_api_url(human_url)
        expected_results = "http://vocabsservices.getty.edu/AATService.asmx/AATGetSubject?subjectID=300046020"
        self.assertEqual(actual_results, expected_results)

    def test_02_passing_wrong_authority_yields_none(self):
        """ test_02_passing_wrong_authority_yeilds_none """
        actual_results = expand_aat_terms({"authority": "Wrong", "term": "anything"})
        self.assertEqual(actual_results, None)

    def test_03_get_term_from_string(self):
        """ test_03_get_term_from_string"""
        term_string = " headdresses [300046023]"
        actual_results = _get_term_from_string(term_string)
        expected_results = "headdresses"
        self.assertEqual(actual_results, expected_results)

    def test_04_get_uri_from_string(self):
        """ test_04_get_uri_from_string"""
        term_string = " headdresses [300046023]"
        actual_results = _get_uri_from_string(term_string)
        expected_results = 'http://vocab.getty.edu/aat/300046023'
        self.assertEqual(actual_results, expected_results)

    def test_05_parse_parent_string_into_broader_terms(self):
        """ test_05_parse_parent_string_into_broader_terms"""
        parent_string = "headdresses [300046023], headgear [300209285], accessories by location on the head [300211601], " \
                        + "worn costume accessories [300209274], costume accessories [300209273], costume (mode of fashion) [300178802], " \
                        + "Costume (hierarchy name) [300209261], Furnishings and Equipment (hierarchy name) [300264551], Objects Facet [300264092]"
        actual_results = _parse_parent_string_into_broader_terms(parent_string)
        expected_results = [
            {'authority': 'AAT', 'term': 'headdresses', 'uri': 'http://vocab.getty.edu/aat/300046023', 'parentTerm': 'headgear'},
            {'authority': 'AAT', 'term': 'headgear', 'uri': 'http://vocab.getty.edu/aat/300209285', 'parentTerm': 'accessories by location on the head'},
            {'authority': 'AAT', 'term': 'accessories by location on the head', 'uri': 'http://vocab.getty.edu/aat/300211601', 'parentTerm': 'worn costume accessories'},
            {'authority': 'AAT', 'term': 'worn costume accessories', 'uri': 'http://vocab.getty.edu/aat/300209274', 'parentTerm': 'costume accessories'},
            {'authority': 'AAT', 'term': 'costume accessories', 'uri': 'http://vocab.getty.edu/aat/300209273', 'parentTerm': 'costume (mode of fashion)'},
            {'authority': 'AAT', 'term': 'costume (mode of fashion)', 'uri': 'http://vocab.getty.edu/aat/300178802'}
        ]
        self.assertEqual(actual_results, expected_results)

    def test_05_5_parse_parent_string_into_broader_terms(self):
        """ test_05_5_parse_parent_string_into_broader_terms"""
        parent_string = "North American [300134121], American regions [300107946], Americas, The [300016617], styles, periods, and cultures by region [300111079], Styles and Periods (hierarchy name) [300015646], Styles and Periods Facet [300264088]"  # noqa: E501
        actual_results = _parse_parent_string_into_broader_terms(parent_string)
        expected_results = [
            {"authority": "AAT", "term": "North American", "uri": "http://vocab.getty.edu/aat/300134121", "parentTerm": "American regions"},
            {"authority": "AAT", "term": "American regions", "uri": "http://vocab.getty.edu/aat/300107946", "parentTerm": "Americas, The"},
            {"authority": "AAT", "term": "Americas, The", "uri": "http://vocab.getty.edu/aat/300016617"}
        ]
        self.assertEqual(actual_results, expected_results)

    @patch('pipelineutilities.expand_getty_aat_terms._get_xml_string_given_url')
    def test_06_get_xml_string_given_url(self, mock_get_xml_string_given_url):
        """ text_06 basically test the whole process given a mocked xml """
        with open(local_folder + 'getty_aat_term_3000046020.xml', 'r') as xml_file:
            xml_string = xml_file.read()
        mock_get_xml_string_given_url.return_value = xml_string
        sample_subject = {
            "authority": "AAT",
            "term": "crowns",
            "uri": "http://vocab.getty.edu/aat/999"
        }
        actual_results = expand_aat_terms(sample_subject)
        expected_results = {
            'authority': 'AAT', 'term': 'crowns', 'uri': 'http://vocab.getty.edu/aat/999',
            'description': 'Ornamental fillets, wreaths, or similar encircling ornaments for the head worn to signify rank, for personal adornment, or as a mark of honor or achievement; also, coronal wreaths of leaves or flowers.',  # noqa: E501
            'broaderTerms': [
                {'authority': 'AAT', 'term': 'headdresses', 'uri': 'http://vocab.getty.edu/aat/300046023', 'parentTerm': 'headgear'},
                {'authority': 'AAT', 'term': 'headgear', 'uri': 'http://vocab.getty.edu/aat/300209285', 'parentTerm': 'accessories by location on the head'},
                {'authority': 'AAT', 'term': 'accessories by location on the head', 'uri': 'http://vocab.getty.edu/aat/300211601', 'parentTerm': 'worn costume accessories'},
                {'authority': 'AAT', 'term': 'worn costume accessories', 'uri': 'http://vocab.getty.edu/aat/300209274', 'parentTerm': 'costume accessories'},
                {'authority': 'AAT', 'term': 'costume accessories', 'uri': 'http://vocab.getty.edu/aat/300209273', 'parentTerm': 'costume (mode of fashion)'},
                {'authority': 'AAT', 'term': 'costume (mode of fashion)', 'uri': 'http://vocab.getty.edu/aat/300178802'},
                {'authority': 'AAT', 'term': 'ceremonial costume', 'uri': 'http://vocab.getty.edu/aat/300210387', 'parentTerm': 'costume by function'},
                {'authority': 'AAT', 'term': 'costume by function', 'uri': 'http://vocab.getty.edu/aat/300212133', 'parentTerm': 'costume (mode of fashion)'},
                {'authority': 'AAT', 'term': 'costume (mode of fashion)', 'uri': 'http://vocab.getty.edu/aat/300178802'}
            ],
            'parentTerms': ['headdresses', 'ceremonial costume'],
            'variants': ['crown'],
        }
        self.assertEqual(actual_results, expected_results)

    @patch('pipelineutilities.expand_getty_aat_terms._get_xml_string_given_url')
    def test_07_get_xml_string_given_url(self, mock_get_xml_string_given_url):
        """ text_07 basically test the whole process given a mocked xml """
        with open(local_folder + 'getty_aat_term_300250130.xml', 'r') as xml_file:
            xml_string = xml_file.read()
        mock_get_xml_string_given_url.return_value = xml_string
        sample_subject = {
            "authority": "AAT",
            "term": "Canis familiaris (species)",
            "uri": "http://vocab.getty.edu/aat/9999"
        }
        actual_results = expand_aat_terms(sample_subject)
        expected_results = {
            'authority': 'AAT',
            'term': 'Canis familiaris (species)',
            'uri': 'http://vocab.getty.edu/aat/9999',
            'description': 'Domesticated species of Canis, formerly believed to be descended from Canis lupus, gray wolves; however, recent studies suggest that gray wolves and domestic dogs instead have a common ancestor, now extinct. Dogs come in a variety of shapes, and sizes and over 400 distinct breeds. All domestic or feral dogs are descended from a single common ancestral species. For more than 12,000 years, it has lived with humans as a hunting companion, protector, and as herders and guardians of sheep, goats, and cattle. Today, dogs are employed as aids to the blind and disabled, or for police work. In Western art, dogs have generally come to symbolize faithfulness and loyalty, however, in other cultures they can symbolizes a varity of traits, ranging from unclean to seducer.',  # noqa: E501
            'broaderTerms': [
                {'authority': 'AAT', 'term': 'Canis (genus)', 'uri': 'http://vocab.getty.edu/aat/300265714', 'parentTerm': 'Canidae (family)'},
                {'authority': 'AAT', 'term': 'Canidae (family)', 'uri': 'http://vocab.getty.edu/aat/300265713', 'parentTerm': 'Carnivora (order)'},
                {'authority': 'AAT', 'term': 'Carnivora (order)', 'uri': 'http://vocab.getty.edu/aat/300310328', 'parentTerm': 'Mammalia (class)'},
                {'authority': 'AAT', 'term': 'Mammalia (class)', 'uri': 'http://vocab.getty.edu/aat/300265707', 'parentTerm': 'Vertebrata (subphylum)'},
                {'authority': 'AAT', 'term': 'Vertebrata (subphylum)', 'uri': 'http://vocab.getty.edu/aat/300265720', 'parentTerm': 'Chordata (phylum)'},
                {'authority': 'AAT', 'term': 'Chordata (phylum)', 'uri': 'http://vocab.getty.edu/aat/300266513', 'parentTerm': 'Animalia (kingdom)'},
                {'authority': 'AAT', 'term': 'Animalia (kingdom)', 'uri': 'http://vocab.getty.edu/aat/300249395', 'parentTerm': 'Eukaryota (domain)'},
                {'authority': 'AAT', 'term': 'Eukaryota (domain)', 'uri': 'http://vocab.getty.edu/aat/300265677', 'parentTerm': 'living organisms (entities)'},
                {'authority': 'AAT', 'term': 'living organisms (entities)', 'uri': 'http://vocab.getty.edu/aat/300390503'}
            ],
            'parentTerms': ['Canis (genus)'],
            'variants': ['dogs', 'dog'],
        }
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
