import time
import json
import os
from transform_marc_json import TransformMarcJson
from convert_json_to_csv import ConvertJsonToCsv
from pymarc import MARCReader
from sentry_sdk import capture_exception
import requests
from pipelineutilities.s3_helpers import write_s3_file
from pipelineutilities.add_files_to_json_object import AddFilesToJsonObject
from pipelineutilities.add_paths_to_json_object import AddPathsToJsonObject
from pipelineutilities.fix_creators_in_json_object import FixCreatorsInJsonObject
from pipelineutilities.save_standard_json import save_standard_json


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
            r = requests.get(url, stream=True)
            if r.status_code == 200:
                marc_records_stream = r.raw
        except ConnectionRefusedError:
            capture_exception('Connection refused on url ' + url)
        except ConnectionError:
            capture_exception('ConnectionError when trying to call url ' + url)
        except:  # noqa E722 - intentionally ignore warning about bare except
            capture_exception('Error caught trying to process url ' + url)
        return marc_records_stream

    def process_marc_records_from_stream(self, test_mode_flag: bool = False) -> int:
        processed_records_count = 0
        try:
            marc_reader = MARCReader(self.marc_records_stream)
            # marc_reader = MARCReader(open('./test/mikala1.bib.mrk', 'rb'), to_unicode=True, force_utf8=True)
            # marc_reader = MARCReader(open('./test/marble.mrc', 'rb'), to_unicode=True, force_utf8=True)
        except TypeError:
            capture_exception('TypeError reading from marc_records_stream.  The Aleph server may be down.')
            return processed_records_count
        transform_marc_json_class = TransformMarcJson(self.config["csv-field-names"])
        add_files_to_json_object_class = AddFilesToJsonObject(self.config)
        add_paths_to_json_object_class = AddPathsToJsonObject(self.config)
        fix_creators_in_json_object_class = FixCreatorsInJsonObject(self.config)
        for marc_record in marc_reader:
            marc_record_as_json = json.loads(marc_record.as_json())
            json_record = transform_marc_json_class.build_json_from_marc_json(marc_record_as_json)
            if json_record:
                json_record = add_files_to_json_object_class.add_files(json_record)
                json_record = add_paths_to_json_object_class.add_paths(json_record)
                json_record = fix_creators_in_json_object_class.fix_creators(json_record)
                self._save_json_record(json_record)
                _export_json_as_csv(self.config, json_record)  # Note: hopefully we can remove this soon.
                processed_records_count += 1
                print("Aleph identifier ", json_record.get("id", ""), " - ", int(time.time() - self.start_time), " seconds.")
            if False:  # change to True to output test files locally.
                filename = self._save_local_marc_json_for_testing(marc_record_as_json)
                self._save_local_standard_json_for_testing(filename, json_record)
            if test_mode_flag:
                break
        if not self.event['local']:
            print("Saved to s3: ", os.path.join(self.config['process-bucket'], self.config['process-bucket-csv-basepath']))  # noqa: #501
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
            else:
                self._save_json_locally(json_file_name, json_record)

    def _save_json_locally(self, json_file_name: str, json_record: dict):
        directory = self.temporary_local_path + "/json"
        if not os.path.exists(directory):
            os.makedirs(directory)
        fully_qualified_file_name = os.path.join(directory, json_file_name)
        with open(os.path.join('test', fully_qualified_file_name), "w") as file1:
            file1.write(json.dumps(json_record, indent=2))


def _export_json_as_csv(config: dict, standard_json: dict):
    """ I'm leaving this here for now until we no longer need to create a CSV from the standard_json """
    convert_json_to_csv_class = ConvertJsonToCsv(config["csv-field-names"])
    csv_string = convert_json_to_csv_class.convert_json_to_csv(standard_json)
    s3_csv_file_name = os.path.join(config['process-bucket-csv-basepath'], standard_json["id"] + '.csv')
    write_s3_file(config['process-bucket'], s3_csv_file_name, csv_string)
