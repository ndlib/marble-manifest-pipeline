#!/usr/bin/env python
"""This module converts images to pyramid tiffs using libvips."""

import sys
import os
import json
from pyvips import Image, Error
import boto3
from botocore.exceptions import ClientError


class ImageProcessor():
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

    Variables:
        bucket (str): name of S3 bucket to pull/push data
        img_read_path (str): path in S3 bucket to pull images
        img_write_path (str): path in S3 bucket to push pytiffs
        report_write_path (str): path in S3 bucket to push image reports
        image_data_file (str): name of file with image data
        image_info (dict): filename -> {image stats}
        __filename (str): name of file currently processing
        image_errs (dict): filename -> {error message}
        previous_run_data (dict): image data, if any, from prior runthrough

    Args:
        config (dict): S3 config information (bucketname, paths, id, etc)
    """
    PYTIF_TILE_WIDTH = 512
    PYTIF_TILE_HEIGHT = 512
    COMPRESSION_TYPE = 'deflate'
    MAX_IMG_HEIGHT = 8500.0
    MAX_IMG_WIDTH = 8500.0
    S3_CLIENT = boto3.client('s3')
    S3_RESOURCE = boto3.resource('s3')

    def __init__(self, config: dict) -> None:
        self.bucket = config['process-bucket']
        self.img_read_path = config['process-bucket-read-basepath'] + '/' \
            + config['id'] + '/images/'
        self.img_write_path = config['process-bucket-write-basepath'] + '/' \
            + config['id'] + '/images/'
        self.report_write_path = config['process-bucket-write-basepath'] + '/' \
            + config['id'] + '/'
        self.image_data_file = config['image-data-file']
        self.image_info = {}
        self.__filename = None
        self.image_errs = {}
        self.previous_run_data = {}

    def list_images(self) -> dict:
        """
        Read contents of S3 bucket images directory.

        Returns:
            dict: The contents of the S3 bucket directory
        """
        kwargs = {'Bucket': self.bucket, 'Prefix': self.img_read_path}
        return self.S3_CLIENT.list_objects_v2(**kwargs)['Contents']

    def _download_s3_file(self, s3_file: str, **kwargs) -> None:
        """
        Download S3 bucket object to local file system.

        Args:
            s3_file: S3 path/key to dowload
            local_file: file to save to
        """
        local_file = kwargs.get('local_file', None)
        if not local_file:
            local_file = os.path.basename(s3_file)
        self.S3_CLIENT.download_file(self.bucket, s3_file, local_file)

    def set_previous_run_data(self):
        """
        Download pre-existing image data
        """
        try:
            self._download_s3_file(self.report_write_path + self.image_data_file)
            with open(self.image_data_file) as previous_run_file:
                self.previous_run_data = json.load(previous_run_file)
        except ClientError as ce:
            if ce.response['Error']['Code'] == '404':
                print(f"No previous image data found for {self.report_write_path}")
            else:
                print(f"Unexpected error: {ce.response['Error']['Code']}")
        except IOError as ioe:
            print(f"Unexpected error handling file: {ioe}")

    def processed_previously(self, image_etag: str) -> bool:
        """
        Return true if the MD5 hash of the current image matches the MD5 hash
        of the image of a previous runthrough.

        Args:
            image_etag: MD5 hash of current image
        Returns:
            bool: True if MD5 hash is equal to prior runs MD5 hash
        """
        origin_md5sum = self.previous_run_data.get(self.__filename, {}).get('origin_md5sum')
        self._set_image_stat('origin_md5sum', image_etag)
        if origin_md5sum == self._get_image_stat('origin_md5sum'):
            self.image_info[self.__filename] = self.previous_run_data.get(self.__filename)
            return True
        return False

    def download_image(self, s3_file: str, local_file: str) -> None:
        """
        Download S3 bucket image to local file system.

        Args:
            s3_file: S3 path/key to dowload
            local_file: file to save to
        """
        self._download_s3_file(s3_file, local_file=local_file)

    def upload_image(self, local_file: str, s3_file: str) -> None:
        """
        Upload local file to S3 bucket path/key.

        Args:
            local_file: local file to upload
            s3_file: S3 path/key
        """
        s3_file = self.img_write_path + s3_file
        self.S3_RESOURCE.Bucket(self.bucket).upload_file(local_file, s3_file)

    def upload_image_reports(self) -> None:
        """
        Uploads image data and, if applicable, image error files
        to an S3 destination.
        """
        with open(self.image_data_file, 'w') as outfile:
            json.dump(self.image_info, outfile)
        s3_file = self.report_write_path + self.image_data_file
        print(f'Uploading {self.image_data_file} to {s3_file}')
        self.S3_RESOURCE.Bucket(self.bucket).upload_file(self.image_data_file, s3_file)

        if self.image_errs:
            image_err_file = "image_err.json"
            s3_file = self.report_write_path + image_err_file
            with open(image_err_file, 'w') as outfile:
                json.dump(self.image_errs, outfile)
            print(f'Uploading {image_err_file} to {s3_file}')
            self.S3_RESOURCE.Bucket(self.bucket).upload_file(image_err_file, s3_file)

    def record_img_err(self, file: str, err_msg: str) -> None:
        """
        Log image processing errors for auditing

        Args:
            file: filename of problematic file
            err_msg: error that occurred while processing file
        """
        self.image_errs[file] = err_msg

    def _preprocess_image(self, file: str):
        """
        Perform any preprocess work on the source image
        prior to transforming that image into a pytif

        Args:
            file: full path/name of local file
        Returns:
            Image: Vips object of source file
        """
        image = Image.new_from_file(file, access='sequential')
        if image.height > self.MAX_IMG_HEIGHT or image.width > self.MAX_IMG_WIDTH:
            if image.height >= image.width:
                shrink_by = image.height / self.MAX_IMG_HEIGHT
            else:
                shrink_by = image.width / self.MAX_IMG_WIDTH
            print(f'Resizing original image by: {shrink_by}')
            print(f'Original image height: {image.height}')
            print(f'Original image width: {image.width}')
            image = image.shrink(shrink_by, shrink_by)
        return image

    def set_filename(self, filename) -> None:
        self.__filename = filename

    def _set_image_stat(self, key: str, stat) -> None:
        """
        Record various image attributes.

        Args:
            key: image attribute name to record
            stat: value of the image attribute
        """
        if self.image_info.get(self.__filename):
            self.image_info[self.__filename][key] = stat
        else:
            self.image_info[self.__filename] = {}
            self.image_info[self.__filename][key] = stat

    def _get_image_stat(self, key: str):
        """
        Retrieve various image attributes.

        Args:
            key: image attribute to retrieve
        """
        return self.image_info[self.__filename].get(key, None)

    def generate_pytiff(self, source_file: str, tif_filename: str) -> None:
        """
        Create a pyramid tiff from a source image, while enforcing constraints,
        and record the image attributes.

        Args:
            source_file: full path/name of local file
            tif_filename: name of the generated pytiff
        """
        image = self._preprocess_image(source_file)
        image.tiffsave(tif_filename, tile=True, pyramid=True, compression=self.COMPRESSION_TYPE,
                       tile_width=self.PYTIF_TILE_WIDTH, tile_height=self.PYTIF_TILE_HEIGHT)
        self._set_image_stat('height', image.get('height'))
        self._set_image_stat('width', image.get('width'))


if __name__ == "__main__":
    img_proc = ImageProcessor(json.loads(sys.argv[1]))
    img_proc.set_previous_run_data()
    for obj in img_proc.list_images():
        try:
            # ignore directories
            if obj['Key'].endswith(('/')):
                continue
            file = os.path.basename(obj['Key'])
            filename, file_ext = os.path.splitext(file)
            tif_file = filename + '.tif'
            img_proc.set_filename(filename)
            # avoid processing unchanged images that have been through
            # the pipeline on a previous runthrough
            if img_proc.processed_previously(obj['ETag'].strip('\"')):
                print(f"No changes to image; reusing existing {tif_file}")
                continue
            # prefix needed to avoid tif naming conflicts
            temp_file = 'TEMP_' + file
            print(f"Downloading {obj['Key']} to {temp_file}")
            img_proc.download_image(obj['Key'], temp_file)
            print(f'Generating pyramid tif for: {file}')
            img_proc.generate_pytiff(temp_file, tif_file)
            print(f'Uploading {tif_file} to {img_proc.img_write_path}')
            img_proc.upload_image(tif_file, tif_file)
            os.remove(temp_file)
            os.remove(tif_file)
        except ClientError as ce:
            print(f"Boto3 exception with {obj['Key']}")
            img_proc.record_img_err(file, str(ce))
        except Error as v:
            print(f"Vips exception with {obj['Key']}")
            img_proc.record_img_err(file, str(v))
        except Exception as e:
            print(f"Exception with {obj['Key']}")
            img_proc.record_img_err(file, str(e))
    try:
        img_proc.upload_image_reports()
    except Exception as e:
        print(f'Couldnt write data/error file(s) to bucket: {str(e)}')
