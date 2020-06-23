# handler.py
""" Module to launch application """

import _set_path  # noqa
import os
import json
from google.oauth2 import service_account  # noqa: E402
from googleapiclient.discovery import build, Resource


class HarvestGoogleSheetInfo():
    """ This retrieves information from a google sheet """
    def __init__(self, google_credentials):
        self.google_credentials = google_credentials
        self.local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"

    def harvest_google_spreadsheet_info(self, site_name: str, google_spreadsheet_id: str, columns_to_export: list, field_name_for_key: str) -> dict:
        """ Retrieve contents of sheet as dict """
        google_credentials_object = self._get_credentials_from_service_account_info(self.google_credentials, ['https://spreadsheets.google.com/feeds'])
        service = build('sheets', 'v4', credentials=google_credentials_object)
        sheets_list = self._get_list_of_sheet_names(service, google_spreadsheet_id)
        spreadsheet_json = self._get_contents_of_spreadsheet(service, site_name, google_spreadsheet_id, sheets_list, columns_to_export, field_name_for_key)
        return spreadsheet_json

    def _get_credentials_from_service_account_info(self, google_credentials: dict, scope: list):
        """ Return credentials given service account file and assumptions of scopes needed """
        # Scopes are defined here:  https://developers.google.com/identity/protocols/googlescopes
        SCOPES = scope
        credentials = service_account.Credentials.from_service_account_info(google_credentials, scopes=SCOPES)
        return(credentials)

    def _get_list_of_sheet_names(self, service: Resource, spreadsheet_id: str) -> list:
        """ Return a list of sheet names within the spreadsheet """
        sheet_names_list = []
        sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        for sheet in sheet_metadata.get('sheets', ''):
            title = sheet.get("properties", {}).get("title", "")
            if title:
                sheet_names_list.append(title)
        return sheet_names_list

    def _get_contents_of_spreadsheet(self, service: Resource, site_name: str, google_spreadsheet_id: str, sheets_list: list, columns_to_export: list, field_name_for_key: str):
        """ Return json containing a key for each sheet name, and a value of the contents of that sheet """
        spreadsheet_json = {}
        sheet_service = service.spreadsheets()
        for sheet_name in sheets_list:
            result = sheet_service.values().get(spreadsheetId=google_spreadsheet_id, range=sheet_name).execute()
            sheet_contents_list = result.get('values', [])
            sheet_json = self._get_json_from_sheet_contents_list(sheet_contents_list, columns_to_export, field_name_for_key)
            spreadsheet_json[sheet_name] = sheet_json
            self._save_json_for_this_sheet(site_name, sheet_name, sheet_json)
        return spreadsheet_json

    def _get_json_from_sheet_contents_list(self, sheet_contents_list: list, columns_to_export: list, field_name_for_key: str):
        """ Convert sheet_contents_list to json """
        json_node = {}
        for column_name in columns_to_export:
            column_name = column_name.lower()
        if sheet_contents_list:
            for i, row in enumerate(sheet_contents_list):
                if i == 0:
                    field_names_list = row
                else:
                    this_row = {}
                    j = 0
                    while j < len(row):
                        field_name = field_names_list[j].lower()
                        if field_name in columns_to_export:
                            if field_name == field_name_for_key:
                                key = row[j].lower()
                            else:
                                this_row[field_name] = row[j]
                        j += 1
                    json_node[key] = this_row
        return json_node

    def _save_json_for_this_sheet(self, site_name: str, sheet_name: str, sheet_json: dict):
        """ save json in folder for site_name in file named for sheet_name. """
        sites_dir = self.local_folder + 'sites'
        if not os.path.isdir(sites_dir):
            os.mkdir(sites_dir)
        this_site_dir = sites_dir + '/' + site_name
        if not os.path.isdir(this_site_dir):
            os.mkdir(this_site_dir)
        with open(this_site_dir + "/" + sheet_name.lower() + ".json", "w") as output_file:
            json.dump(sheet_json, output_file, indent=2, ensure_ascii=False)
