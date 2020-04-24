#!/usr/bin/python3
"""This module converts images to pyramid tiffs using libvips."""

import sys
import json
import boto3
from s3_helpers import upload_json
from processor_factory import ProcessorFactory
from csv_collection import load_csv_data
from pipeline_config import load_pipeline_config


class ImageRunner():
    def __init__(self, config: dict) -> None:
        self.ids = config['ids']
        self.bucket = config['process-bucket']
        self.csv_read_base = config['process-bucket-csv-basepath']
        self.img_write_base = config['process-bucket-read-basepath']
        self.img_file = config['image-data-file']
        self.gdrive_ssm = f"{config['google_keys_ssm_base']}/credentials"
        self.csv_config = config
        self.processor = None

    def process_images(self) -> None:
        for id in self.ids:
            print(id)
            print(load_csv_data(id, self.csv_config))
            id_results = {}
            for file in load_csv_data(id, self.csv_config).files():
                print(file)
                if not self.processor:
                    processor_info = self._get_processor_info(file.get('filePath'))
                    src = processor_info.get('type')
                    cred = processor_info.get('cred', None)
                    self.processor = ProcessorFactory().get_processor(src, cred=cred)
                img_config = {
                    'collection_id': id,
                    'bucket': self.bucket,
                    'img_write_base': self.img_write_base
                }
                self.processor.set_data(file, img_config)
                id_results.update(self.processor.process())
            s3_file = f"{self.img_write_base}/{id}/{self.img_file}"
            upload_json(self.bucket, s3_file, id_results)

    def _get_processor_info(self, filepath: str) -> dict:
        img_type = {'type': 's3'}
        if filepath.startswith('https://drive.google'):
            img_type = {'type': 'gdrive', 'cred': _get_credentials(self.gdrive_ssm)}
        elif filepath.startswith('http://bendo'):
            img_type = {'type': 'bendo'}
        return img_type


def _get_credentials(ssm_key: str) -> dict:
    ssm = boto3.client('ssm')
    parameter = ssm.get_parameter(Name=ssm_key, WithDecryption=True)
    return json.loads(parameter['Parameter']['Value'])


if __name__ == "__main__":
    try:
        event = json.loads(sys.argv[1])
        config = load_pipeline_config(event)
        runner = ImageRunner(config)
        runner.process_images()
    except Exception as e:
        print(e)
