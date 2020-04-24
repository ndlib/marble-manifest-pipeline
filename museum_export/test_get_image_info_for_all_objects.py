# test_get_image_info_for_all_objects.py
""" test get_image_info_for_all_objects """
import unittest
from get_image_info_for_all_objects import GetImageInfoForAllObjects


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def test_1_get_image_files_list(self):
        """ test_1 _get_image_files_list """
        objects = {
            "abc": {"id": "abc", "digitalAssets": [{"fileDescription": "file1.jpg"}, {"fileDescription": "file2.jpg"}]},
            "abc.a": {"id": "abc.a", "digitalAssets": [{"fileDescription": "file3.jpg"}, {"fileDescription": "file4.jpg"}]},
            "abc.b": {"id": "abc.b", "digitalAssets": [{"fileDescription": "file5.jpg"}, {"fileDescription": "file6.jpg"}]}
        }
        get_image_info_for_all_objects_class = GetImageInfoForAllObjects({}, {}, "")
        image_files_list = get_image_info_for_all_objects_class._get_image_files_list(objects)
        expected_object = ['file1.jpg', 'file2.jpg', 'file3.jpg', 'file4.jpg', 'file5.jpg', 'file6.jpg']
        self.assertTrue(image_files_list == expected_object)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
