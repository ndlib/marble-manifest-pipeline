import time
import json
import os
from transform_marc_json import TransformMarcJson
from dependencies.pymarc import MARCReader
from dependencies.sentry_sdk import capture_exception
import dependencies.requests
from dependencies.pipelineutilities.search_files import crawl_available_files
from dependencies.pipelineutilities.s3_helpers import write_s3_file


class HarvestAlephMarc():
    """ This performs all Marc-related processing """
    def __init__(self, config, event, marc_records_url):
        self.config = config
        self.marc_records_url = marc_records_url
        self.start_time = time.time()
        self.event = event
        self.hash_of_available_files = {}
        if not self.event['local']:
            self.hash_of_available_files = crawl_available_files(self.config)
            # with open(os.path.join('test', 'hash_of_available_files.json'), "w") as file1:
            #     file1.write(json.dumps(self.hash_of_available_files, indent=2, default=str))
        self.temporary_local_path = '/tmp'
        self.marc_records_stream = self._open_marc_records_stream()

    def _open_marc_records_stream(self):
        """ Return marc records from URL."""
        marc_records_stream = b""  # MARCReader requires a byte string
        url = self.marc_records_url
        try:
            r = dependencies.requests.get(url, stream=True)
            if r.status_code == 200:
                marc_records_stream = r.raw
        except ConnectionRefusedError:
            capture_exception('Connection refused on url ' + url)
        except ConnectionError:
            capture_exception('ConnectionError when trying to call url ' + url)
        except:  # noqa E722 - intentionally ignore warning about bare except
            capture_exception('Error caught trying to process url ' + url)
        return marc_records_stream

    def process_marc_records_from_stream(self, test_mode_flag=False):
        processed_records_count = 0
        try:
            marc_reader = MARCReader(self.marc_records_stream)
        except TypeError:
            capture_exception('TypeError reading from marc_records_stream.  The Aleph server may be down.')
            return processed_records_count
        transform_marc_json_class = TransformMarcJson(self.config["csv-field-names"], self.hash_of_available_files)
        for marc_record in marc_reader:
            marc_record_as_json = json.loads(marc_record.as_json())
            json_record = transform_marc_json_class.build_json_from_marc_json(marc_record_as_json)
            if False:  # change to True to output test files locally.
                filename = self._save_local_marc_json_for_testing(marc_record_as_json)
                self._save_local_nd_json_for_testing(filename, json_record)
            if json_record:
                csv_string = transform_marc_json_class.create_csv_from_json(json_record)
                self._save_csv_record(json_record, csv_string)
                self._save_json_record(json_record)
                processed_records_count += 1
                print("Aleph identifier ", json_record.get("id", ""), " - ", int(time.time() - self.start_time), " seconds.")
            if test_mode_flag:
                break
        if not self.event['local']:
            print("Saved to s3: ", os.path.join(self.config['process-bucket'], self.config['process-bucket-csv-basepath']))  # noqa: #501
        return processed_records_count

    def _save_local_marc_json_for_testing(self, marc_record_as_json):
        for field in marc_record_as_json['fields']:
            if '001' in field:
                filename = field['001'] + '.json'
                with open(os.path.join('test', filename), "w") as file1:
                    file1.write(json.dumps(marc_record_as_json, indent=2))
        return filename

    def _save_local_nd_json_for_testing(self, filename, json_record):
        filename = filename.replace(".json", "_nd.json")
        with open(os.path.join('test', filename), "w") as file1:
            file1.write(json.dumps(json_record, indent=2))

    def _save_csv_record(self, json_record, csv_string):
        if 'id' in json_record:
            csv_file_name = json_record['id'] + '.csv'
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

    def _save_json_record(self, json_record):
        if 'id' in json_record:
            json_file_name = json_record['id'] + '.json'
            if not self.event['local']:
                self._save_json_to_s3(self.config['process-bucket'], json_file_name, json.dumps(json_record), json_record['id'])
            else:
                self._save_json_locally(json_file_name, json_record)

    def _save_json_to_s3(self, s3_bucket_name, json_file_name, json_string, json_record_id):
        fully_qualified_file_name = os.path.join("json/" + json_record_id, json_file_name)
        try:
            write_s3_file(s3_bucket_name, fully_qualified_file_name, json_string)
            results = True
        except Exception:
            results = False
        return results

    def _save_json_locally(self, json_file_name, json_record):
        directory = self.temporary_local_path + "/json"
        if not os.path.exists(directory):
            os.makedirs(directory)
        fully_qualified_file_name = os.path.join(directory, json_file_name)
        with open(os.path.join('test', fully_qualified_file_name), "w") as file1:
            file1.write(json.dumps(json_record, indent=2))
