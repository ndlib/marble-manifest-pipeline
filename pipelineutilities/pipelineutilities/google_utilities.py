# google_utilities.py
""" Common code to perform basic google calls

In order to use this, add the following to requirements.txt

    oauth2client==4.1.3
    google==2.0.2
    google-api-python-client==1.7.11
    google-auth-httplib2==0.0.3
    google-auth-oauthlib==0.4.0    """
from googleapiclient.http import MediaIoBaseDownload  # noqa: E402  # MediaFileUpload
from googleapiclient.errors import HttpError
import io
from google.oauth2 import service_account  # noqa: E402
from googleapiclient.discovery import build  # noqa: E402
from datetime import datetime, timedelta
import socket


def _get_credentials_from_service_account_info(google_credentials):
    """ Return credentials given service account file and assumptions of scopes needed """
    service_account_info = google_credentials
    credentials = ""
    # print(service_account_info)
    # Scopes are defined here:  https://developers.google.com/identity/protocols/googlescopes
    SCOPES = ['https://www.googleapis.com/auth/drive']
    credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    return(credentials)


def establish_connection_with_google_api(google_credentials):
    """ Create a connection to the Google API """
    timeout_in_sec = 60 * 3  # 3 minutes timeout limit
    socket.setdefaulttimeout(timeout_in_sec)  # This will set the default timeout to 3 minutes for build call below
    credentials = _get_credentials_from_service_account_info(google_credentials)
    google_connection = build('drive', 'v3', credentials=credentials)
    return google_connection


def execute_google_query(google_connection, drive_id, query_string):
    """ Execute the Google API query defined elsewhere """
    accumulated_results = []
    nextPageToken = ""
    try:
        while True:  # Loop until there are no more results from our query
            results = google_connection.files().list(
                pageSize=1000,  # 1000 is the maximum pageSize allowed
                pageToken=nextPageToken,
                fields="kind, nextPageToken, incompleteSearch, files(id, name, mimeType, parents, driveId, size, md5Checksum, modifiedTime)",  # noqa: E501
                supportsAllDrives="true",  # required if writing to a team drive
                driveId=drive_id,
                includeItemsFromAllDrives="true",  # required if querying from a team drive
                corpora="drive",
                q=query_string).execute()
            items = results.get('files', [])
            accumulated_results.extend(items)  # append list of changed items to our growing list
            if 'nextPageToken' not in results:
                break  # No more results from our query, so get out
            else:
                nextPageToken = results['nextPageToken']
    except HttpError:
        accumulated_results = []
        print('Http Error executing google query: ' + query_string)

    return accumulated_results


def download_google_file(google_connection, file_id, local_file_name):
    request = google_connection.files().get_media(fileId=file_id)
    fh = io.FileIO(local_file_name, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        # print("Download %d%%." % int(status.progress() * 100))


def build_google_query_string(parent_folder_id, hours_threshold, mime_type, file_name_fragment=""):
    """ Create a query string given parameters passed """
    query_string = ""
    if hours_threshold == "":
        hours_threshold = 0
    recent_past = datetime.utcnow() - timedelta(hours=int(hours_threshold))  # Google uses UTC time
    recent_past_string = recent_past.strftime('%Y-%m-%dT%H:%M:%S')
    time_phrase = "" if hours_threshold == 0 else " and modifiedTime > '" + recent_past_string + "'"
    parent_folder_phrase = "" if parent_folder_id == '' else " and '" + parent_folder_id + "' in parents"
    mime_type_phrase = "" if mime_type == '' else " and mimeType contains '" + mime_type + "'"
    file_name_phrase = "" if file_name_fragment == "" else " and name contains '" + file_name_fragment + "'"
    query_string = "trashed = False" \
        + time_phrase \
        + parent_folder_phrase \
        + mime_type_phrase \
        + file_name_phrase
    return query_string


def get_parent_folders_given_folder_name(google_connection, drive_id, exact_folder_name):
    parent_folders = []
    query_string = "trashed = False " \
        + " and mimeType = 'application/vnd.google-apps.folder' " \
        + " and name = '" + exact_folder_name + "'"
    parent_folders = execute_google_query(google_connection, drive_id, query_string)
    return parent_folders
