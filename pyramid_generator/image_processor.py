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
    MAX_IMG_HEIGHT = 8500.0
    MAX_IMG_WIDTH = 8500.0

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

    @abstractmethod
    def process(self) -> dict:
        pass

    def set_data(self, config: dict) -> None:
        self.id = config['id']
        self.source_image = config['file']
        self.filename = config['filename']
        self.ext = config['ext']
        self.local_file = f"TEMP_{self.filename}{self.ext}"
        self.tif_file = f"{self.filename}.tif"
        self.bucket = config['bucket']
        self.img_write_base = config['img_write_base']
        self.source_md5sum = config['md5sum']

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

    def _cleanup(self) -> None:
        """
        Remove temporary data/images
        """
        os.remove(self.local_file)
        os.remove(self.tif_file)

    def _previously_processed(self) -> bool:
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
        """
        Create a pyramid tiff from a source image, while enforcing constraints,
        and record the image attributes.

        Args:
            file: local file to transform into pytiff
            tif_filename: name of the resulting pytiff file
        """
        image = self._preprocess_image(file)
        image.tiffsave(tif_filename, tile=True, pyramid=True, compression=self.COMPRESSION_TYPE,
                       tile_width=self.PYTIF_TILE_WIDTH, tile_height=self.PYTIF_TILE_HEIGHT)
        # print(f"{image.get_fields()}")  # image fields, including exif
        self._log_result('height', image.get('height'))
        self._log_result('width', image.get('width'))
        self._log_result('md5sum', self.source_md5sum)

    def _preprocess_image(self, file: str) -> Image:
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
