""" test_expand_getty_ia_terms """
import _set_path  # noqa  # pylint: disable=import-error, unused-import
import os  # pylint: disable=wrong-import-order
from pipelineutilities.expand_getty_ia_terms import _get_id_given_url, _get_broader_terms_given_id, expand_ia_terms
import unittest  # pylint: disable=wrong-import-order
from unittest.mock import patch  # pylint: disable=wrong-import-order


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


class Test(unittest.TestCase):
    """ Test expand_getty_aat_terms """

    def test_01_get_id_given_url(self):
        """ test_01_get_id_given_url """
        human_url = "http://vocab.getty.edu/page/ia/901000032"
        actual_results = _get_id_given_url(human_url)
        expected_results = "901000032"
        self.assertEqual(actual_results, expected_results)

    @patch('pipelineutilities.expand_getty_ia_terms._get_xml_string_given_url')
    def test_02_get_broader_terms_given_id(self, mock_get_xml_string_given_url):
        """ verify _get_broader terms_given_id """
        with open(local_folder + 'getty_ia_term_901000032_parent_hierarchy.xml', 'r') as xml_file:
            xml_string = xml_file.read()
        mock_get_xml_string_given_url.return_value = xml_string
        actual_results, actual_parent_terms = _get_broader_terms_given_id("901000032")
        expected_results = [
            {'authority': 'IA', 'term': 'Legend, Religion, Mythology', 'uri': '901000002'},
            {'authority': 'IA', 'term': 'Christian iconography', 'uri': '901000024', 'parentTerm': 'Legend, Religion, Mythology'},
            {'authority': 'IA', 'term': 'Christian characters', 'uri': '901000047', 'parentTerm': 'Christian iconography'},
            {'authority': 'IA', 'term': 'Blessed Virgin Mary (Christian character)', 'uri': '901000032', 'parentTerm': 'Christian characters'}
        ]
        expected_parent_terms = ['Christian characters']
        self.assertEqual(actual_results, expected_results)
        self.assertEqual(actual_parent_terms, expected_parent_terms)

    @patch('pipelineutilities.expand_getty_ia_terms._get_xml_string_given_url')
    def test_03_test_everything_but_broader_terms(self, mock_get_xml_string_given_url):
        """ test_03_test_everything_but_broader_terms """
        with open(local_folder + 'getty_ia_term_901000032.xml', 'r') as xml_file:
            xml_string = xml_file.read()
        mock_get_xml_string_given_url.return_value = xml_string
        sample_subject_json = {
            "authority": "IA",
            "term": "Blessed Virgin Mary",
            "uri": "http://vocab.getty.edu/page/ia/999",
        }
        actual_results = expand_ia_terms(sample_subject_json)
        expected_results = {
            'authority': 'IA',
            'term': 'Blessed Virgin Mary',
            'uri': 'http://vocab.getty.edu/page/ia/999',
            'description': 'Mother of Jesus, venerated since the earliest days of Christianity. Christians believe the birth of Jesus was a virgin birth, his having been conceived through the Holy Spirit.',  # pylint: disable=line-too-long
            'variants': [
                'Mary, Blessed Virgin, Saint (Christian character)',
                'Blessed Virgin (Christian character)',
                'Blessed Virgin, Saint (Christian character)',
                'Virgin Mary (Christian character)'
                ]
            }
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
