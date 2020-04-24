# find_google_images.py
from pipelineutilities.google_utilities import execute_google_query, establish_connection_with_google_api


class FindGoogleImages():
    """ This class retrieves Google file information given passed list of files.
        Init Parameters:  google_credentials json object, as well as the drive_id to be searched """

    def __init__(self, google_credentials, drive_id):
        self.google_credentials = google_credentials
        self.drive_id = drive_id
        self.image_files = {}

    def get_image_file_info(self, image_files_list):
        """ Get a file information given a list of files which we need to find on Google drive """
        self.image_files = {}
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
            query_string = self._build_google_query_string(image_files_list)
            google_connection = establish_connection_with_google_api(self.google_credentials)
            query_results = execute_google_query(google_connection, self.drive_id, query_string)
            self._save_file_info_to_hash(query_results)
        return self.image_files

    def _build_google_query_string(self, image_files_list):
        """ Given the list of image files to search, return the Google query string needed """
        query_string = "trashed = False and mimeType contains 'image' and ("
        first_pass = True
        for image_file_name in image_files_list:
            if not first_pass:
                query_string += " or "
            query_string += " name = '" + image_file_name + "'"
            first_pass = False
        query_string += ")"
        return query_string

    def _save_file_info_to_hash(self, query_results):
        """ Save query results into hash for later use """
        for record in query_results:
            self.image_files[record['name']] = record
        return self.image_files
