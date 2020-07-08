import os
import json
import boto3
from pyvips import Image
from abc import ABC, abstractmethod
from botocore.exceptions import ClientError


class ImageProcessor(ABC):
    S3_CLIENT = boto3.client('s3')
    S3_RESOURCE = boto3.resource('s3')
    PYTIF_TILE_WIDTH = 512
    PYTIF_TILE_HEIGHT = 512
    COMPRESSION_TYPE = 'deflate'

    def __init__(self) -> None:
        super().__init__()
        self.id = None
        self.source_image = None
        self.filename = None
        self.ext = None
        self.local_file = None
        self.tif_file = None
        self.bucket = None
        self.img_write_base = None
        self.source_md5sum = None
        self.prior_results = {}
        self.image_result = {}
        self.max_img_height = 4000.0
        self.max_img_width = 4000.0
        # self.max_img_height = 8500.0
        # self.max_img_width = 8500.0
        self.copyrighted = False

    @abstractmethod
    def process(self) -> dict:
        pass

    def _is_copyrighted(self, usage: str) -> bool:
        if usage and usage.lower().startswith('copyright'):
            return True

    def _log_result(self, key: str, info) -> None:
        if self.image_result.get(self.filename):
            self.image_result[self.filename][key] = info
        else:
            self.image_result[self.filename] = {}
            self.image_result[self.filename][key] = info

    def _cleanup(self) -> None:
        os.remove(self.local_file)
        os.remove(self.tif_file)

    def _previously_processed(self) -> bool:
        if self.copyrighted:
            return False

        if self.id not in self.prior_results:
            self.prior_results.update({self.id: {}})
            img_data = f"{self.img_write_base}/{self.id}/image_data.json"
            self._set_prior_run_data(self.bucket, img_data, self.id)
        prior_md5sum = self.prior_results.get(self.id).get(self.filename, {}).get('md5sum', 'nomatch')

        if self.source_md5sum == prior_md5sum:
            return True
        return False

    def _set_prior_run_data(self, bucket: str, key: str, id: str, **kwargs) -> dict:
        try:
            local_file = os.path.basename(key)
            boto3.resource('s3').Bucket(bucket).download_file(key, local_file)
            with open(local_file) as json_file:
                self.prior_results.get(id).update(json.load(json_file))
            os.remove(local_file)
        except ClientError as ce:
            if ce.response['Error']['Code'] == '404':
                print(f"No previous image data found for {bucket}://{key}")
            else:
                print(f"Unexpected error {bucket}://{key}: {ce.response['Error']['Code']}")

    def _generate_pytiff(self, file: str, tif_filename: str) -> None:
        image = self._preprocess_image(file)
        # print("image xres, yres = ", image.xres, image.yres, image.bands, image.format, image.interpretation)
        dpi_resolution_multiplier = 25.3999998062134
        print("image was originally ", image.xres * dpi_resolution_multiplier, "x", image.yres * dpi_resolution_multiplier, "dpi")
        image.tiffsave(tif_filename, tile=True, pyramid=True, compression=self.COMPRESSION_TYPE,
                       tile_width=self.PYTIF_TILE_WIDTH, tile_height=self.PYTIF_TILE_HEIGHT)
        # print(f"{image.get_fields()}")  # image fields, including exif
        self._log_result('height', image.get('height'))
        self._log_result('width', image.get('width'))
        self._log_result('md5sum', self.source_md5sum)
        # print("self.image_result = ", self.image_result)

    def _preprocess_image(self, file: str) -> Image:
        image = Image.new_from_file(file, access='sequential')
        if image.height > self.max_img_height or image.width > self.max_img_width:
            if image.height >= image.width:
                shrink_by = image.height / self.max_img_height
            else:
                shrink_by = image.width / self.max_img_width
            print(f'Resizing original image by: {shrink_by}')
            print(f'Original image height: {image.height}')
            print(f'Original image width: {image.width}')

            image = image.shrink(shrink_by, shrink_by)
            print("New image height = ", image.height)
            print("New image width = ", image.width)
        return image

    def set_data(self, img_data: dict, config: dict) -> None:
        self.id = config.get('collection_id')
        self.source_image = img_data.get('filePath')
        self.file_path, self.ext = os.path.splitext(self.source_image)
        self.filename = os.path.basename(self.file_path)
        self.ext = f".{self.ext}"
        self.local_file = f"TEMP_{self.filename}{self.ext}"
        self.tif_file = f"{self.filename}.tif"
        self.bucket = config.get('bucket')
        self.img_write_base = config.get('img_write_base')
        self.source_md5sum = img_data.get('md5Checksum', None)
        # if copyrighted work scale height/width directed by aamd.org
        if self._is_copyrighted(img_data.collection().get('copyrightStatus')):
            self.copyrighted = True
            self.max_img_height = 560.0
            self.max_img_width = 843.0
