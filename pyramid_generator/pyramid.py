#!/usr/bin/python3
"""This module converts images to pyramid tiffs using libvips."""

from abc import ABC, abstractmethod
import sys
import os
import json
from pyvips import Image
import boto3
from botocore.exceptions import ClientError
from csv_collection import load_csv_data


class ImageRunner():
    def __init__(self, config: dict) -> None:
        self.src_bucket = config['process-bucket']
        self.csv_read_base = config['process-bucket-csv-basepath']
        self.ids = config['ids']
        self.dest_bucket = config['process-bucket']
        self.dest_write_path = config['process-bucket-read-basepath']

    def process_images(self) -> None:
        for id in self.ids:
            id_results = {}
            config = {'csv-data-files-bucket': self.src_bucket, 'csv-data-files-basepath': self.csv_read_base}
            for file in load_csv_data(id, config).files():
                if file.get("filePath").startswith('s3'):
                    #if file.get("filePath") == "s3://testlibnd-junk/collections/ead_xml/images/BPP_1001/BPP_1001-001.jpg" or file.get("filePath") == "s3://testlibnd-junk/collections/ead_xml/images/BPP_1001/BPP_1001-001-F2.jpg":
                    id_results.update(self._process_s3_image(id, file))
            s3_file = f"{self.dest_write_path}/{id}/image_data.json"
            self._upload_image_report(id_results, s3_file)

    def _process_s3_image(self, id, file) -> dict:
        image_info = {}
        image_info['id'] = id
        image_info['file'] = file.get("filePath")
        image_info['md5sum'] = file.get("etag")
        image_info['bucket'] = self.dest_bucket
        image_info['bucket_write_path'] = self.dest_write_path
        return self.S3ImageProcessor(image_info).process()

    def _upload_image_report(self, report_data, s3_file) -> None:
        """
        Uploads image data and, if applicable, image error files
        to an S3 destination.
        """
        report_file = "image_data.json"
        with open(report_file, 'w') as outfile:
            json.dump(report_data, outfile)
        boto3.resource('s3').Bucket(self.dest_bucket).upload_file(report_file, s3_file)

    class ImageProcessor(ABC):
        S3_CLIENT = boto3.client('s3')
        S3_RESOURCE = boto3.resource('s3')
        PYTIF_TILE_WIDTH = 512
        PYTIF_TILE_HEIGHT = 512
        COMPRESSION_TYPE = 'deflate'
        MAX_IMG_HEIGHT = 8500.0
        MAX_IMG_WIDTH = 8500.0

        def __init__(self, config: dict) -> None:
            super().__init__()
            self.id = config['id']
            self.source_image = config['file']
            self.filename, self.ext = self.source_image.split('/')[-1].split('.')
            self.ext = f".{self.ext}"
            self.bucket = config['bucket']
            self.bucket_write_path = config['bucket_write_path']
            self.source_md5sum = config['md5sum']
            self.image_result = {}

        @abstractmethod
        def process(self) -> dict:
            pass

        def _log_result(self, key: str, info) -> None:
            """
            Log image progress

            Args:
                key: lookup value
                info: object to note
            """
            if self.image_result.get(self.filename):
                self.image_result[self.filename][key] = info
            else:
                self.image_result[self.filename] = {}
                self.image_result[self.filename][key] = info

    class S3ImageProcessor(ImageProcessor):
        def __init__(self, config: dict) -> None:
            super().__init__(config)

        def process(self) -> dict:
            if self._previously_processed():
                self._log_result('status', 'processed')
                self._log_result('reason', 'no changes to image since last run')
            else:
                bucket, key = self.source_image.split('/', 2)[-1].split('/', 1)
                local_file = f"{self.filename}{self.ext}"
                s3_file = f"{self.bucket_write_path}/{self.id}/images/{self.filename}.tif"
                print(bucket)
                print(key)
                print(local_file)
                self.S3_RESOURCE.Bucket(bucket).download_file(key, local_file)
                print("POST DL")
                self._generate_pytiff()
                self.S3_RESOURCE.Bucket(bucket).upload_file(local_file, s3_file)
                self._cleanup()
                self._log_result('status', 'processed')
            return self.image_result

        def _previously_processed(self) -> bool:
            # has this been ran before and has it changed?
            previous_image = f"{self.bucket_write_path}/{self.id}/images/{self.filename}"
            response = self._get_image_metadata(self.bucket, previous_image)
            return self.source_md5sum == response['ETag']

        def _get_image_metadata(self, bucket: str, key: str, **kwargs) -> dict:
            try:
                response = self.S3_CLIENT.head_object(Bucket=bucket, Key=key)
            except ClientError as ce:
                response = {'ETag': None}
                if ce.response['Error']['Code'] == '404':
                    print(f"No previous image data found for {bucket}://{key}")
                else:
                    print(f"Unexpected error {bucket}://{key}: {ce.response['Error']['Code']}")
            return response

        def _generate_pytiff(self) -> None:
            """
            Create a pyramid tiff from a source image, while enforcing constraints,
            and record the image attributes.
            """
            print("ENTER")
            image = self._preprocess_image(f"{self.filename}{self.ext}")
            print("POST PREPROCESS")
            image.tiffsave(f"{self.filename}.tif", tile=True, pyramid=True, compression=self.COMPRESSION_TYPE,
                           tile_width=self.PYTIF_TILE_WIDTH, tile_height=self.PYTIF_TILE_HEIGHT)
            print("POST IMAGE PROCESS")
            # print(f"{image.get_fields()}")  # image fields, including exif
            self._log_result('height', image.get('height'))
            self._log_result('width', image.get('width'))

        def _preprocess_image(self, file: str):
            """
            Perform any preprocess work on the source image
            prior to transforming that image into a pytif

            Args:
                file: full path/name of local file
            Returns:
                Image: Vips object of source file
            """
            print("LOAD PRE")
            image = Image.new_from_file(file, access='sequential')
            print("POST LOAD IN PRE")
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

        def _cleanup(self) -> None:
            os.remove(f"{self.filename}{self.ext}")
            os.remove(f"{self.filename}.tif")

    class GDImageProcessor(ImageProcessor):
        def __init__(self, config: dict) -> None:
            super().__init__(config)
            self.source_md5 = config['md5']

        def process(self) -> dict:
            return {}


if __name__ == "__main__":
    try:
        runner = ImageRunner(json.loads(sys.argv[1]))
        runner.process_images()
    except Exception as e:
        print(e)
