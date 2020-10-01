"""
Harvest Aleph Marc
This retrieves from a url a marc file consisting of marc records to process.
This translates each of these marc records into a standard json file
and saves it to the manifest bucket.
"""

import time
import json
import os
import requests
from sentry_sdk import capture_exception
from transform_marc_json import TransformMarcJson
from pipelineutilities.add_files_to_json_object import AddFilesToJsonObject
from pipelineutilities.standard_json_helpers import StandardJsonHelpers
from pipelineutilities.save_standard_json import save_standard_json
from pipelineutilities.save_standard_json_to_dynamo import SaveStandardJsonToDynamo
from pymarc import MARCReader


class HarvestAlephMarc():
    """ This performs all Marc-related processing """
    def __init__(self, config, event, marc_records_url):
        self.config = config
        self.marc_records_url = marc_records_url
        self.start_time = time.time()
        self.event = event
        self.temporary_local_path = '/tmp'
        self.marc_records_stream = self._open_marc_records_stream()
        # self.marc_records_stream = open('./test/mikala1.bib.mrk', 'rb')

    def _open_marc_records_stream(self):
        """ Return marc records from URL."""
        marc_records_stream = b""  # MARCReader requires a byte string
        url = self.marc_records_url
        try:
            results = requests.get(url, stream=True)
            if results.status_code == 200:
                marc_records_stream = results.raw
        except ConnectionRefusedError:
            capture_exception('Connection refused on url ' + url)
        except ConnectionError:
            capture_exception('ConnectionError when trying to call url ' + url)
        except:  # noqa E722 - intentionally ignore warning about bare exceptfrom pymarc import MARCReader 
            capture_exception('Error caught trying to process url ' + url)
        return marc_records_stream

    def process_marc_records_from_stream(self, test_mode_flag: bool = False) -> int:
        """ Process each marc record read from the stream """
        processed_records_count = 0
        try:
            marc_reader = MARCReader(self.marc_records_stream)
            # marc_reader = MARCReader(open('./test/mikala1.bib.mrk', 'rb'), to_unicode=True, force_utf8=True)
            # marc_reader = MARCReader(open('./test/marble.mrc', 'rb'), to_unicode=True, force_utf8=True)
        except TypeError:
            capture_exception('TypeError reading from marc_records_stream.  The Aleph server may be down.')
            return processed_records_count
        transform_marc_json_class = TransformMarcJson()
        add_files_to_json_object_class = AddFilesToJsonObject(self.config)
        standard_json_helpers_class = StandardJsonHelpers(self.config)
        for marc_record in marc_reader:
            marc_record_as_json = json.loads(marc_record.as_json())
            json_record = transform_marc_json_class.build_json_from_marc_json(marc_record_as_json)
            if json_record:
                print("Aleph identifier ", json_record.get("id", ""), " - ", int(time.time() - self.start_time), " seconds.")
                json_record = add_files_to_json_object_class.add_files(json_record)
                json_record = standard_json_helpers_class.enhance_standard_json(json_record)
                self._save_json_record(json_record)
                processed_records_count += 1
            if False:  # change to True to output test files locally.from pymarc import MARCReader
                filename = self._save_local_marc_json_for_testing(marc_record_as_json)
                self._save_local_standard_json_for_testing(filename, json_record)
            if test_mode_flag:
                break
        if not self.event['local']:
            print("Saved to s3: ", os.path.join(self.config['process-bucket'], self.config['process-bucket-data-basepath']))  # noqa: #501
        return processed_records_count

    def _save_local_marc_json_for_testing(self, marc_record_as_json: dict) -> str:
        for field in marc_record_as_json['fields']:
            if '001' in field:
                filename = field['001'] + '.json'
                with open(os.path.join('test', filename), "w") as file1:
                    file1.write(json.dumps(marc_record_as_json, indent=2))
        return filename

    def _save_local_standard_json_for_testing(self, filename: str, json_record: dict):
        filename = filename.replace(".json", "_standard.json")
        with open(os.path.join('test', filename), "w") as file1:
            file1.write(json.dumps(json_record, indent=2, sort_keys=True))

    def _save_json_record(self, json_record: dict):
        if 'id' in json_record:
            json_file_name = json_record['id'] + '.json'
            if not self.event['local']:
                save_standard_json(self.config, json_record)
                save_standard_json_to_dynamo_class = SaveStandardJsonToDynamo(self.config)
                save_standard_json_to_dynamo_class.save_standard_json(json_record)
            else:
                self._save_json_locally(json_file_name, json_record)

    def _save_json_locally(self, json_file_name: str, json_record: dict):
        directory = self.temporary_local_path + "/json"
        if not os.path.exists(directory):
            os.makedirs(directory)
        fully_qualified_file_name = os.path.join(directory, json_file_name)
        with open(os.path.join('test', fully_qualified_file_name), "w") as file1:
            file1.write(json.dumps(json_record, indent=2))
