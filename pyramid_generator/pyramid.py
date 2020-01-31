#!/usr/bin/python3
"""This module converts images to pyramid tiffs using libvips."""

import sys
import json
import boto3
from image_processor import S3ImageProcessor
from csv_collection import load_csv_data


class ImageRunner():
    def __init__(self, config: dict) -> None:
        self.ids = config['ids']
        self.bucket = config['process-bucket']
        self.csv_read_base = config['process-bucket-csv-basepath']
        self.img_write_base = config['process-bucket-read-basepath']
        self.s3_processor = S3ImageProcessor()

    def process_images(self) -> None:
        for id in self.ids:
            id_results = {}
            config = {'process-bucket': self.bucket, 'process-bucket-csv-basepath': self.csv_read_base}
            for file in load_csv_data(id, config).files():
                if file.get("filePath").startswith('s3'):
                    id_results.update(self._process_s3_image(id, file))
            s3_file = f"{self.img_write_base}/{id}/image_data.json"
            self._upload_image_report(id_results, s3_file)

    def _process_s3_image(self, id, file) -> dict:
        image_info = {}
        image_info['id'] = id
        image_info['file'] = file.get("filePath")
        image_info['md5sum'] = file.get("etag")
        image_info['bucket'] = self.bucket
        image_info['img_write_base'] = self.img_write_base
        self.s3_processor.set_data(image_info)
        return self.s3_processor.process()

    def _upload_image_report(self, report_data, s3_file) -> None:
        """
        Uploads image data and, if applicable, image error files
        to an S3 destination.
        """
        report_file = "image_data.json"
        with open(report_file, 'w') as outfile:
            json.dump(report_data, outfile)
        boto3.resource('s3').Bucket(self.bucket).upload_file(report_file, s3_file)


if __name__ == "__main__":
    try:
        runner = ImageRunner(json.loads(sys.argv[1]))
        runner.process_images()
    except Exception as e:
        print(e)
