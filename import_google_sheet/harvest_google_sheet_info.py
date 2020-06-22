# handler.py
""" Module to launch application """

import _set_path  # noqa
import os
import json
# from pathlib import Path
# from process_web_kiosk_json_metadata import processWebKioskJsonMetadata  # noqa: E402
from pipelineutilities.pipeline_config import load_config_ssm  # noqa: E402
# import sentry_sdk  # noqa: E402
# from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration  # noqa: E402
# from pipelineutilities.google_utilities import execute_google_query, establish_connection_with_google_api
from google.oauth2 import service_account  # noqa: E402
from googleapiclient.discovery import build


class HarvestGoogleSheetInfo():
    """ This retrieves information from a google sheet """
    def __init__(self, config):
        self.config = config
        self.local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"

    def harvest_google_sheet_info(self, spreadsheet_id: str) -> dict:
        """ Retrieve contents of sheet as dict """
        spreadsheet_json = {}
        google_config = load_config_ssm(self.config['google_keys_ssm_base'])
        self.config.update(google_config)
        if "museum-google-credentials" not in self.config:
            print("No google-credentials exist in config.")
            return spreadsheet_json
        google_credentials = json.loads(self.config["museum-google-credentials"])
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        google_credentials_object = self._get_credentials_from_service_account_info(google_credentials, scope)

        service = build('sheets', 'v4', credentials=google_credentials_object)

        # Call the Sheets API
        # spreadsheet_id = '1gKUkoG921EW0AAa-9c58Yn3wGyx8UXLnlFCWHs3G7E4'
        sheets_list = self._get_list_of_sheet_names(service, spreadsheet_id)
        spreadsheet_json = self._get_contents_of_sheet(service, spreadsheet_id, sheets_list)
        return spreadsheet_json

    def _get_contents_of_sheet(self, service, spreadsheet_id, sheets_list):
        spreadsheet_json = {}
        sheet_service = service.spreadsheets()
        for sheet_name in sheets_list:
            result = sheet_service.values().get(spreadsheetId=spreadsheet_id, range=sheet_name).execute()
            values_from_sheet = result.get('values', [])
            spreadsheet_json[sheet_name] = values_from_sheet
        return spreadsheet_json

    def _get_list_of_sheet_names(self, service, spreadsheet_id: str) -> list:
        sheet_names = []
        sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        # sheets = sheet_metadata.get('sheets', '')
        for sheet in sheet_metadata.get('sheets', ''):
            title = sheet.get("properties", {}).get("title", "")
            if title:
                sheet_names.append(title)
        return sheet_names

    def _get_credentials_from_service_account_info(self, google_credentials: dict, scope: list):
        """ Return credentials given service account file and assumptions of scopes needed """
        service_account_info = google_credentials
        credentials = ""
        # print(service_account_info)
        # Scopes are defined here:  https://developers.google.com/identity/protocols/googlescopes
        SCOPES = scope
        credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
        return(credentials)
