# find_google_images.py
import time
from dependencies.pipelineutilities.google_utilities import execute_google_query


class FindGoogleImages():
    """ This class accepts a JSON object representing one item of museum content and massages that to fit our needs for processing. """

    def __init__(self, config, google_connection, start_time):
        self.config = config
        self.google_connection = google_connection
        self.start_time = start_time
        self.image_files = {}

    def get_image_file_info(self, composite_json):
        """ Get a list of files which we need to find on Google drive """
        self.image_files = {}
        image_files_list = []
        image_files_to_find = 0
        if 'objects' in composite_json:
            for object in composite_json['objects']:
                if 'digitalAssets' in object:
                    for digital_asset in object['digitalAssets']:
                        image_files_list.append(digital_asset['fileDescription'])
                        image_files_to_find += 1
        print(image_files_to_find, " images to locate on Google Drive.", int(time.time() - self.start_time), 'seconds.')
        self._find_images_in_chunks(image_files_list)
        return self.image_files

    def _find_images_in_chunks(self, image_files_list):
        """ Because Google queries are limited, we have to chunk this process """
        first_item_to_process = 0
        chunk_size = 50
        while first_item_to_process < len(image_files_list):
            last_item_to_process = first_item_to_process + chunk_size
            if last_item_to_process > len(image_files_list):
                last_item_to_process = len(image_files_list)
            list_to_process = []
            list_to_process = image_files_list[first_item_to_process:last_item_to_process]
            self._find_images_in_google_drive(list_to_process)
            first_item_to_process += chunk_size

    def _find_images_in_google_drive(self, image_files_list):
        """ Go find the list of files from Google drive """
        if len(image_files_list) > 0:
            query_string = "trashed = False and mimeType contains 'image' and ("
            first_pass = True
            for image_file_name in image_files_list:
                if not first_pass:
                    query_string += " or "
                query_string += " name = '" + image_file_name + "'"
                first_pass = False
            query_string += ")"
            drive_id = self.config['museum-google-drive-id']
            results = execute_google_query(self.google_connection, drive_id, query_string)
            for record in results:
                self.image_files[record['name']] = record
        return self.image_files
