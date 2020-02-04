from image_processor import ImageProcessor
from pyvips import Image


class S3ImageProcessor(ImageProcessor):
    def __init__(self) -> None:
        super().__init__()

    def process(self) -> dict:
        if self._previously_processed():
            file_info = self.prior_results.get(self.id).get(self.filename)
            height = file_info.get('height', 2000)
            width = file_info.get('width', 2000)
            md5sum = file_info.get('md5sum', '')
            self._log_result('status', 'processed')
            self._log_result('height', height)
            self._log_result('width', width)
            self._log_result('md5sum', md5sum)
            self._log_result('reason', 'no changes to image since last run')
        else:
            img_bucket, key = self.source_image.split('/', 2)[-1].split('/', 1)
            s3_file = f"{self.img_write_base}/{self.id}/images/{self.tif_file}"
            self.S3_RESOURCE.Bucket(img_bucket).download_file(key, self.local_file)
            self._generate_pytiff(self.local_file, self.tif_file)
            self.S3_RESOURCE.Bucket(self.bucket).upload_file(self.tif_file, s3_file)
            self._cleanup()
            self._log_result('status', 'processed')
        return self.image_result

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
