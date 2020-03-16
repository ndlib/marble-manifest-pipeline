from s3_processor import S3ImageProcessor
from gdrive_processor import GoogleImageProcessor


class ProcessorFactory():
    @staticmethod
    def get_processor(format, **kwargs):
        if format == 's3':
            return S3ImageProcessor()
        elif format == 'gdrive':
            return GoogleImageProcessor(kwargs.get('cred'))
