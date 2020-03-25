from s3_processor import S3ImageProcessor
from gdrive_processor import GoogleImageProcessor
from bendo_processor import BendoImageProcessor


class ProcessorFactory():
    @staticmethod
    def get_processor(format, **kwargs):
        if format == 's3':
            return S3ImageProcessor()
        elif format == 'gdrive':
            return GoogleImageProcessor(kwargs.get('cred'))
        elif format == 'bendo':
            return BendoImageProcessor()
