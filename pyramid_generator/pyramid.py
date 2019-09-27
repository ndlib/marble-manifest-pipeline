#!/usr/bin/python
"""This module converts images to pyramid tiffs using libvips."""

import sys
import os
from pyvips import Image
import json
import boto3
import botocore


class ImageProcessor:
    """
    This class is responsible for gathering images from a source,
    transforming them to pytiffs, and sending them to a destination.
    All image measurements are in pixels.
    Attributes:
        PYTIF_TILE_HEIGHT   PyTiff tile height
        PYTIF_TILE_WIDTH    PyTiff tile width
        COMPRESSION_TYPE    PyTiff compression factor
        MAX_IMG_HEIGHT      Maximum image height before resizing(must be float)
        MAX_IMG_WIDTH       Maximum image width before resizing(must be float)
        S3_CLIENT           boto3 s3 client obj
        S3_RESOURCE         boto3 s3 resource obj
    """
    PYTIF_TILE_WIDTH = 512
    PYTIF_TILE_HEIGHT = 512
    COMPRESSION_TYPE = 'deflate'
    MAX_IMG_HEIGHT = 8500.0
    MAX_IMG_WIDTH = 8500.0
    S3_CLIENT = boto3.client('s3')
    S3_RESOURCE = boto3.resource('s3')

    def __init__(self, config={}):
        self.bucket = config['process-bucket']
        self.img_read_path = config['process-bucket-read-basepath'] + '/' \
            + config['id'] + '/images/'
        self.img_write_path = config['process-bucket-write-basepath'] + '/' \
            + config['id'] + '/images/'
        self.report_write_path = config['process-bucket-write-basepath'] + '/' \
            + config['id'] + '/'
        self.image_info = {}
        self.image_errs = {}

    def list_images(self):
        kwargs = {'Bucket': self.bucket, 'Prefix': self.img_read_path}
        return self.S3_CLIENT.list_objects_v2(**kwargs)['Contents']

    def download_image(self, s3_file, local_file):
        self.S3_CLIENT.download_file(self.bucket, s3_file, local_file)

    def upload_image(self, s3_file, local_file):
        s3_file = self.img_write_path + s3_file
        self.S3_RESOURCE.Bucket(self.bucket).upload_file(local_file, s3_file)

    def upload_image_reports(self):
        image_data_file = 'image_data.json'
        with open(image_data_file, 'w') as outfile:
            json.dump(self.image_info, outfile)
        s3_file = self.report_write_path + image_data_file
        print('Uploading {} to {}'.format(image_data_file, s3_file))
        self.S3_RESOURCE.Bucket(self.bucket).upload_file(image_data_file, s3_file)

        if self.image_errs:
            image_err_file = "image_err.json"
            s3_file = self.report_write_path + image_err_file
            with open(image_err_file, 'w') as outfile:
                json.dump(self.image_errs, outfile)
            print('Uploading {} to {}'.format(image_err_file, s3_file))
            self.S3_RESOURCE.Bucket(self.bucket).upload_file(image_err_file, s3_file)

    def record_img_err(self, file, err_msg):
        self.image_errs[file] = err_msg

    def _preprocess_image(self, file):
        image = Image.new_from_file(file, access='sequential')
        if image.height > self.MAX_IMG_HEIGHT or image.width > self.MAX_IMG_WIDTH:
            if image.height >= image.width:
                shrink_by = image.height / self.MAX_IMG_HEIGHT
            else:
                shrink_by = image.width / self.MAX_IMG_WIDTH
            print('Resizing original image by: {}%'.format(shrink_by))
            print('Original image height: {}'.format(image.height))
            print('Original image width: {}'.format(image.width))
            image = image.shrink(shrink_by, shrink_by)
        return image

    def _postprocess_image(self, filename, image):
        self.image_info[filename] = {
            u'height': image.get('height'),
            u'width': image.get('width')
        }

    def generate_pytiff(self, source_file, tif_filename):
        image = self._preprocess_image(source_file)
        image.tiffsave(tif_filename, tile=True, pyramid=True, compression=self.COMPRESSION_TYPE,
                       tile_width=self.PYTIF_TILE_WIDTH, tile_height=self.PYTIF_TILE_HEIGHT)
        self._postprocess_image(tif_filename, image)


if __name__ == "__main__":
    img_proc = ImageProcessor(json.loads(sys.argv[1]))
    for obj in img_proc.list_images():
        file = os.path.basename(obj['Key'])
        filename, file_ext = os.path.splitext(file)
        tif_file = filename + '.tif'
        # ignore directories
        if obj['Key'].endswith(('/')):
            continue

        try:
            # prefix needed to avoid tif naming conflicts
            temp_file = 'TEMP_' + file
            print('Downloading {} to {}'.format(obj['Key'], temp_file))
            img_proc.download_image(obj['Key'], temp_file)
        except Exception as e:
            print('Exception with {}'.format(obj['Key']))
            img_proc.record_img_err(file, str(e))
            continue

        try:
            print('Generating pyramid tif for: {}'.format(file))
            img_proc.generate_pytiff(temp_file, tif_file)
        except Exception as e:
            img_proc.record_img_err(file, str(e))
            continue

        try:
            print('Uploading {} to {}'.format(tif_file, img_proc.img_write_path))
            img_proc.upload_image(tif_file, tif_file)
            os.remove(temp_file)
            os.remove(tif_file)
        except Exception as e:
            img_proc.record_img_err(file, str(e))
            continue

    try:
        img_proc.upload_image_reports()
    except Exception as e:
        print('Couldnt write data/error file(s) to bucket: ' + str(e))
