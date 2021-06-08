"""This module used in a lambda and api gateway to send list of files."""

import _set_path  # noqa
import io
import json
import os
import csv


# python -c 'from generate_file_listing import *; test()'
def test():
    filename = './cache/crawl_available_files_cache.json'
    if os.path.exists(filename):
        with io.open(filename, 'r', encoding='utf-8') as json_file:
            files_json = json.load(json_file)

    with open('MARBLE_RBSC_files_listing.csv', 'w') as csvfile:
        fieldnames = ['fileGroupId', 'directory', 'filename']
        csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        csv_writer.writeheader()

        for _k, v in files_json.items():
            directory = v.get('directory')
            if 'MARBLE-images' not in directory:
                file_id = v.get('fileId')
                for item in v.get('files', []):
                    key = item.get('key')
                    file_name = os.path.basename(key)
                    csv_writer.writerow({'fileGroupId': file_id, 'directory': directory, 'filename': file_name})
