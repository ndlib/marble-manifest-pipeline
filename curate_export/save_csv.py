# save_csv.py
import os
from dependencies.pipelineutilities.s3_helpers import write_s3_file


class SaveCsv():
    def __init__(self, config, event):
        self.config = config
        self.event = event
        self.temporary_local_path = "/tmp"

    def save_csv_file(self, id, csv_string):
        csv_file_name = id + '.csv'
        if not self.event['local']:
            self._save_csv_to_s3(self.config['process-bucket'], csv_file_name, csv_string)
        else:
            self._save_csv_locally(csv_file_name, csv_string)

    def _save_csv_to_s3(self, s3_bucket_name, csv_file_name, csv_string):
        fully_qualified_file_name = os.path.join(self.config['process-bucket-csv-basepath'], csv_file_name)
        try:
            write_s3_file(s3_bucket_name, fully_qualified_file_name, csv_string)
            results = True
        except Exception:
            results = False
        return results

    def _save_csv_locally(self, csv_file_name, csv_string):
        directory = self.temporary_local_path
        if not os.path.exists(directory):
            os.makedirs(directory)
        fully_qualified_file_name = os.path.join(directory, csv_file_name)
        with open(fully_qualified_file_name, "w") as csv_file:
            csv_file.write(csv_string)
