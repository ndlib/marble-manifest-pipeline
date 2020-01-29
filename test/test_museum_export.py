# test_museum_export.py
""" test museum_export """
import os
import sys
import json
where_i_am = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(where_i_am)
sys.path.append(where_i_am)
sys.path.append(parent_dir + "/museum_export")
import unittest  # noqa: E402
from process_web_kiosk_json_metadata import processWebKioskJsonMetadata  # noqa: E402
from dependencies.pipelineutilities.pipeline_config import get_pipeline_config, load_config_ssm  # noqa: E402
from dependencies.pipelineutilities.google_utilities import establish_connection_with_google_api, execute_google_query, build_google_query_string  # noqa: #402


class Test(unittest.TestCase):
    """ Class for test fixtures """
    event = {}
    if 'ssm_key_base' not in event and 'SSM_KEY_BASE' in os.environ:
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']
    if 'ssm_key_base' not in event:
        event['ssm_key_base'] = '/all/manifest-pipeline-v3'
    config = get_pipeline_config(event)
    config = load_config_ssm(config['google_keys_ssm_base'], config)
    config = load_config_ssm(config['museum_keys_ssm_base'], config)

    config['runningUnitTests'] = True
    folder_name = "/tmp"
    file_name = "web_kiosk_mets_composite.xml"
    google_credentials = json.loads(config["museum-google-credentials"])
    google_connection = establish_connection_with_google_api(google_credentials)
    drive_id = config['museum-google-drive-id']
    required_fields = config['museum-required-fields']
    clean_up_as_we_go = False
    web_kiosk_class = processWebKioskJsonMetadata(config, google_connection)

    # Note:  Tests are run alphabetically!  I added numbers in the names to force sorting
    def test_1_establish_connection_with_google_api(self):
        """ Test establishing connection with Google api """
        print('1 - test_1_establish_connection_with_google_api')
        self.assertTrue(self.google_connection)

    def test_2_get_museum_composite_json_metadata(self):
        """ Test retrieving museum composite JSON metadata """
        print('2 - test_2_get_museum_composite_json_metadata')
        complete_json_metadata = self.web_kiosk_class.get_composite_json_metadata("full")
        # print("complete_json_metadata=", complete_json_metadata)
        self.assertTrue(complete_json_metadata != {})

    def test_3_get_image_file_info(self):
        """ Get a list of image files to find for the objects selected """
        print('3 - test_3_get_image_file_info')
        image_file_list = self.web_kiosk_class._get_image_file_info()
        # print("image_file_list=", image_file_list)
        self.assertTrue(image_file_list != [])
        images_in_files = self.web_kiosk_class._find_images_in_google_drive(image_file_list)
        # print("images_in_files=", images_in_files)
        self.assertTrue(images_in_files != {})

    def test_4_process_composite_json_metadata(self):
        """ Process one object in unit test mode to verify end-to-end processing works """
        print('4 - test_4_process_composite_json_metadata')
        records_processed = self.web_kiosk_class.process_composite_json_metadata(True)
        # print("records_processed=", records_processed)
        self.assertTrue(records_processed == 1)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
