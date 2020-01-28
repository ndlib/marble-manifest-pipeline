import os
from abc import ABC, abstractmethod
import boto3
from pyvips import Image
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
        self.ext = None
        self.local_file = None
        self.tif_file = None
        self.bucket = None
        self.img_write_base = None
        self.source_md5sum = None
        self.image_result = {}

    @abstractmethod
    def process(self) -> dict:
        pass

    def set_data(self, config: dict) -> None:
        self.id = config['id']
        self.source_image = config['file']
        self.filename, self.ext = self.source_image.split('/')[-1].rsplit('.', 1)
        self.ext = f".{self.ext}"
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


class S3ImageProcessor(ImageProcessor):
    def __init__(self) -> None:
        super().__init__()

    def process(self) -> dict:
        # if self._previously_processed():
        #     self._log_result('status', 'processed')
        #     self._log_result('reason', 'no changes to image since last run')
        # else:
        img_bucket, key = self.source_image.split('/', 2)[-1].split('/', 1)
        s3_file = f"{self.img_write_base}/{self.id}/images/{self.tif_file}"
        self.S3_RESOURCE.Bucket(img_bucket).download_file(key, self.local_file)
        self._generate_pytiff(self.local_file, self.tif_file)
        self.S3_RESOURCE.Bucket(self.bucket).upload_file(self.tif_file, s3_file)
        self._cleanup()
        self._log_result('status', 'processed')
        return self.image_result

    def _previously_processed(self) -> bool:
        # has this been ran before and has it changed?
        previous_image = f"{self.img_write_base}/{self.id}/images/{self.filename}"
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


class GDImageProcessor(ImageProcessor):
    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self.source_md5 = config['md5']
