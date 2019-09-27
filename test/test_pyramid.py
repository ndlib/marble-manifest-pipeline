import unittest
import os
from shutil import copyfile
from pyramid_generator.pyramid import ImageProcessor
from pyvips import Image
from test.test_utils import load_img_data_for_test


class TestImageMethods(unittest.TestCase):
    def setUp(self):
        config = {
            'process-bucket': 'not_relevant',
            'process-bucket-read-basepath': 'not_relevant',
            'process-bucket-write-basepath': 'not_relevant',
            'id': 'not_relevant'
        }
        self.img_proc = ImageProcessor(config)
        self.img_data = load_img_data_for_test()
        pass

    def test_img_attr(self):
        """dont resize, just verify"""
        for img_id, img_info in self.img_data.items():
            vips_img = self.img_proc._preprocess_image(img_info['path'])
            self.assertEqual(vips_img.height, img_info['height'])
            self.assertEqual(vips_img.width, img_info['width'])

    def test_img_shrink_by_width(self):
        """force shrink image by width"""
        shrink_factor = 2
        path = self.img_data['woman']['path']
        original_width = self.img_data['woman']['width']
        original_height = self.img_data['woman']['height']
        self.img_proc.MAX_IMG_WIDTH = original_width/shrink_factor
        vips_img = self.img_proc._preprocess_image(path)
        self.assertEqual(vips_img.height, original_height/shrink_factor)
        self.assertEqual(vips_img.width, original_width/shrink_factor)

    def test_img_shrink_by_height(self):
        """force shrink image by height"""
        shrink_factor = 2.5
        path = self.img_data['cube']['path']
        original_width = self.img_data['cube']['width']
        original_height = self.img_data['cube']['height']
        self.img_proc.MAX_IMG_HEIGHT = original_height/shrink_factor
        vips_img = self.img_proc._preprocess_image(path)
        self.assertEqual(vips_img.height, original_height/shrink_factor)
        self.assertEqual(vips_img.width, original_width/shrink_factor)

    def test_generage_pytiff(self):
        """generate pyramid tif from image"""
        for img_id, img_info in self.img_data.items():
            file = os.path.basename(img_info['path'])
            filename, file_ext = os.path.splitext(file)
            temp_file = 'TEMP_' + filename + file_ext
            tif_file = filename + '.tif'
            copyfile(img_info['path'], temp_file)
            self.img_proc.generate_pytiff(temp_file, tif_file)
            pytiff_img = Image.new_from_file(tif_file, access='sequential')
            self.assertEqual(pytiff_img.height, img_info['height'])
            self.assertEqual(pytiff_img.width, img_info['width'])
            os.remove(temp_file)
            os.remove(tif_file)


if __name__ == '__main__':
    unittest.main()
