""" test_standard_json_helpers """
import _set_path  # noqa: F401
import json
import os
from pipelineutilities.report_missing_fields import ReportMissingFields
import unittest
from pipelineutilities.pipeline_config import load_pipeline_config
from unittest.mock import patch


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"
config = load_pipeline_config({"local": True, "config-file": "something", "process-bucket": "something"})


class Test(unittest.TestCase):
    """ Test report missing fields """

    def test_01_get_required_fields_for_source_system(self):
        """ test_01_get_required_fields_for_source_system """
        report_missing_fields_class = ReportMissingFields(config)
        actual_results = report_missing_fields_class._get_required_fields_for_source_system("EmbARK")
        expected_results = config['required-fields-by-source-system']['EmbARK']
        self.assertEqual(actual_results, expected_results)

    def test_02_find_missing_fields(self):
        """ test_02_find_missing_fields """
        report_missing_fields_class = ReportMissingFields(config)
        required_fields = report_missing_fields_class._get_required_fields_for_source_system("EmbARK").get("required-fields", {})
        with open(local_folder + '1976.046.json', 'r', encoding='utf-8') as json_file:
            standard_json = json.load(json_file)
        actual_results = report_missing_fields_class.find_missing_fields(standard_json, required_fields)
        expected_results = ''
        self.assertEqual(actual_results, expected_results)
        standard_json.pop('title')
        actual_results = report_missing_fields_class.find_missing_fields(standard_json, required_fields)
        expected_results = 'Title - at json path location title\n'
        self.assertEqual(actual_results, expected_results)

    # Note, this really sends an email.  Uncomment to verify sending an email works correctly, then recomment before saving to github
    # def test_03_email_missing_fields(self):
    #     """ test_03_email_missing_fields """
    #     notify_list = ['smattiso@nd.edu']
    #     item_id = '1976.046'
    #     missing_fields = 'Title - at json path location title\n'
    #     missing_fields_notification = item_id + ' is missing the following required field(s): \n' + missing_fields + '\n'
    #     report_missing_fields_class = ReportMissingFields(config)
    #     report_missing_fields_class.email_missing_fields(notify_list, item_id, missing_fields, missing_fields_notification)

    @patch('pipelineutilities.report_missing_fields._send_email')
    def test_04_process_missing_fields(self, mock_send_email):
        """ test_04_process_missing_fields """
        with open(local_folder + '1976.046.json', 'r', encoding='utf-8') as json_file:
            standard_json = json.load(json_file)
        config['required-fields-by-source-system']['EmbARK']["notify"] = ["smattiso@nd.edu"]
        mock_send_email.return_value = 'email mocked'
        report_missing_fields_class = ReportMissingFields(config)
        standard_json.pop('title')
        actual_results = report_missing_fields_class.process_missing_fields(standard_json, True)
        expected_results = 'Title - at json path location title\n'
        self.assertEqual(actual_results, expected_results)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
