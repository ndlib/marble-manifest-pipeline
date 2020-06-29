# test_create_json_from_xml.py
""" test create_json_from_xml """
import _set_path  # noqa
import os
import json
import unittest
from pipelineutilities.add_files_to_json_object import AddFilesToJsonObject


local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"


def debug_json(tested, result):
    tested = json.dumps(tested, sort_keys=True, indent=2)
    result = json.dumps(result, sort_keys=True, indent=2)

    f = open("./test.json", "w")
    f.write(tested)
    f.close()

    f = open("./result.json", "w")
    f.write(result)
    f.close()


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        pass

    def test_1_test_creating_json_from_xml(self):
        """ test test_creating_json_from_xml """
        with open(local_folder + '/MSNEA8011_EAD.json', 'r') as input_source:
            nd_json = json.load(input_source)
        config = {}
        # config['rbsc-image-bucket'] = "libnd-smb-rbsc"
        config['local'] = True
        add_file_to_json_object = AddFilesToJsonObject(config)
        nd_json_with_files = add_file_to_json_object.add_files(nd_json)
        # with open(local_folder + '/MSNEA8011_EAD_with_files.json', 'w') as f:
        #     json.dump(nd_json_with_files, f, indent=2)
        with open(local_folder + '/MSNEA8011_EAD_with_files.json', 'r') as input_source:
            expected_json = json.load(input_source)

        # sort and remove extra blanks
        debug_json(expected_json, nd_json_with_files)
        
        expected_json = "".join(json.dumps(expected_json, sort_keys=True).split())
        nd_json_with_files = "".join(json.dumps(nd_json_with_files, sort_keys=True).split())
        self.assertEqual(expected_json, nd_json_with_files)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
