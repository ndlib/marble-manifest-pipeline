from image_processor import ImageProcessor
import google_utilities


class GoogleImageProcessor(ImageProcessor):
    def __init__(self, credentials) -> None:
        super().__init__()
        self.gdrive_conn = google_utilities.establish_connection_with_google_api(credentials)

    def set_data(self, img_data: dict, config: dict) -> None:
        self.id = config.get('collection_id')
        self.source_image = img_data.get('fileId')
        self.filename, self.ext = img_data.get('id').rsplit('.', maxsplit=1)
        self.ext = f".{self.ext}"
        self.local_file = f"TEMP_{self.filename}{self.ext}"
        self.tif_file = f"{self.filename}.tif"
        self.bucket = config.get('bucket')
        self.img_write_base = config.get('img_write_base')
        self.source_md5sum = img_data.get('md5Checksum', None)
        # if copyrighted work scale height/width directed by aamd.org
        if self._is_copyrighted(img_data.get('usage')):
            self.copyrighted = True
            self.max_img_height = 560.0
            self.max_img_width = 843.0

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
            google_utilities.download_google_file(self.gdrive_conn, self.source_image, self.local_file)
            self._generate_pytiff(self.local_file, self.tif_file)
            s3_file = f"{self.img_write_base}/{self.id}/images/{self.tif_file}"
            self.S3_RESOURCE.Bucket(self.bucket).upload_file(self.tif_file, s3_file)
            self._cleanup()
            self._log_result('status', 'processed')
        return self.image_result
