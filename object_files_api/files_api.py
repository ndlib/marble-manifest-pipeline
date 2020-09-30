import os
import io
from datetime import datetime, timedelta
import json
import time
from s3_helpers import write_s3_json
from api_helpers import json_serial
from search_files import crawl_available_files


class FilesApi():
    def __init__(self, event, config):
        self.event = event
        self.event['local'] = self.event.get('local', False)
        self.config = config
        self.local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"
        self.time_to_break = datetime.now() + timedelta(seconds=890)
        if self.config.get('test', False):
            self.directory = os.path.join(os.path.dirname(__file__), 'test')
        else:
            self.directory = os.path.join(os.path.dirname(__file__), 'cache')
        self.start_time = time.time()

    def save_files_details(self):
        all_files_listing = self._crawl_available_files_from_s3_or_cache(True)
        file_objects = []
        if not self.event['local']:
            print("saving objectFiles to ", self.config['manifest-server-bucket'] + '/objectFiles')
        for _key, value in all_files_listing.items():
            summary_json = self._convert_directory_to_json(value)
            file_objects.append(summary_json)
            self._save_file_objects_per_collection(value, summary_json)
            if datetime.now() >= self.time_to_break:
                break
        if self.event['local']:
            self._cache_s3_call(os.path.join(self.directory, "file_objects.json"), file_objects)
        else:
            write_s3_json(self.config['manifest-server-bucket'], 'objectFiles/all/index.json', file_objects)
        return file_objects

    def _save_file_objects_per_collection(self, collection_json: dict, summary_json: dict) -> int:
        count = 0
        if "files" in collection_json:
            my_json = dict(summary_json)
            my_json['files'] = []
            for file in collection_json['files']:
                my_json['files'].append(file)
                count += 1
            if not self.event['local']:
                s3_key = os.path.join('objectFiles/', os.path.basename(summary_json['id']), 'index.json')
                print("saving ", os.path.basename(summary_json['uri']), int(time.time() - self.start_time), 'seconds.')
                write_s3_json(self.config['manifest-server-bucket'], s3_key, my_json)
            if self.event.get('test', False):
                filename = os.path.basename(summary_json['uri']) + '.json'
                self._cache_s3_call(os.path.join(self.directory, filename), my_json)
        return count

    def _cache_s3_call(self, file_name: str, objects: dict):
        with open(file_name, 'w') as outfile:
            json.dump(objects, outfile, default=json_serial, sort_keys=True, indent=2)

    def _crawl_available_files_from_s3_or_cache(self, force_use_s3: bool = False) -> dict:
        cache_file = os.path.join(self.directory, 'crawl_available_files_cache.json')
        if force_use_s3 or (not self.config.get("test", False) and not self.config.get('local', False)):
            objects = crawl_available_files(self.config)
            if self.config.get('local', False):
                self._cache_s3_call(cache_file, objects)
            return objects
        elif os.path.exists(cache_file):
            with io.open(cache_file, 'r', encoding='utf-8') as json_file:
                return json.load(json_file)
        else:
            return {}

    def _convert_directory_to_json(self, file: dict) -> dict:
        data = {}
        data['id'] = file.get('fileId')
        data['label'] = os.path.basename(file.get('fileId')).replace("-", " ")
        data['lastModified'] = file.get('lastModified')
        data['path'] = file['sourceType'] + '://' + file['source'] + os.path.dirname(file.get('directory'))
        data['source'] = file.get('source')
        data['sourceType'] = file.get('sourceType')
        data['uri'] = os.path.join('https://presentation-iiif.library.nd.edu/objectFiles/', data['id'])

        count = 0

        if 'files' in file:
            count = len(file['files'])

        data['numberOfFiles'] = count

        return data
