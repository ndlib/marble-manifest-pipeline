# harvest_metadata_rules.py
""" Module to read metadata harvest rules from google spreadsheet """

import _set_path  # noqa
import os
import json
from google.oauth2 import service_account  # noqa: E402
from googleapiclient.discovery import build
from dependencies.sentry_sdk import capture_exception


class HarvestMetadataRules():
    """ This retrieves information from a google sheet """
    def __init__(self, google_credentials: dict, test_mode_flag: bool = False):
        self.google_credentials = google_credentials
        self.local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"
        self.test_mode_flag = test_mode_flag
        if not test_mode_flag:
            google_credentials_object = self._get_credentials_from_service_account_info(self.google_credentials, ['https://spreadsheets.google.com/feeds'])
            self.sheets_service = build('sheets', 'v4', credentials=google_credentials_object)
        else:
            self.sheets_service = None

    def harvest_google_spreadsheet_info(self, site_name: str) -> dict:
        """ Retrieve contents of sheet as dict """
        self.site_to_harvest = site_name.lower()
        control_json = self._read_site_to_harvest_control_json(self.site_to_harvest)
        google_spreadsheet_id = control_json["googleSpreadsheetId"]
        columns_to_export = control_json["columnsToExport"]
        field_name_for_key = control_json["fieldForKey"]
        sheets_list = self._get_list_of_sheet_names(google_spreadsheet_id)
        spreadsheet_json = self._get_contents_of_spreadsheet(site_name, google_spreadsheet_id, sheets_list, columns_to_export, field_name_for_key)
        return spreadsheet_json

    def _read_site_to_harvest_control_json(self, site_to_harvest: str) -> dict:
        site_control_json = {}
        filename = self.local_folder + site_to_harvest + '.json'
        try:
            with open(filename, 'r') as input_source:
                site_control_json = json.load(input_source)
        except FileNotFoundError as e:
            print('Unable to load site_harvest_control_json (' + filename + ').')
            capture_exception(e)
        except EnvironmentError as e:
            capture_exception(e)
        return site_control_json

    def _get_credentials_from_service_account_info(self, google_credentials: dict, scope: list) -> service_account.Credentials:
        """ Return credentials given service account file and assumptions of scopes needed """
        # Scopes are defined here:  https://developers.google.com/identity/protocols/googlescopes
        SCOPES = scope
        credentials = service_account.Credentials.from_service_account_info(google_credentials, scopes=SCOPES)
        return credentials

    def _get_list_of_sheet_names(self, spreadsheet_id: str) -> list:
        """ Return a list of sheet names within the spreadsheet """
        sheet_names_list = []
        sheet_metadata = self.sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        for sheet in sheet_metadata.get('sheets', ''):
            title = sheet.get("properties", {}).get("title", "")
            if title:
                sheet_names_list.append(title)
        return sheet_names_list

    def _get_contents_of_spreadsheet(self, site_name: str, google_spreadsheet_id: str, sheets_list: list, columns_to_export: list, field_name_for_key: str) -> dict:
        """ Return json containing a key for each sheet name, and a value of the contents of that sheet """
        spreadsheet_json = {}
        for sheet_name in sheets_list:
            sheet_contents_list = self._get_contents_of_one_tab_list(google_spreadsheet_id, sheet_name)
            if not self.test_mode_flag:
                filename = self.local_folder + 'test/' + sheet_name.lower() + "_sheet_contents_list.json"
                with open(filename, 'w') as f:
                    f.write(json.dumps(sheet_contents_list))
            sheet_json = self._get_json_from_sheet_contents_list(sheet_contents_list, columns_to_export, field_name_for_key)
            spreadsheet_json[sheet_name.lower()] = sheet_json
            if not self.test_mode_flag:
                self._save_json_for_this_sheet(site_name, sheet_name, sheet_json)
        return spreadsheet_json

    def _get_contents_of_one_tab_list(self, google_spreadsheet_id: str, sheet_name: str) -> dict:
        """ Return json containing a key for each sheet name, and a value of the contents of that sheet """
        sheet_service = self.sheets_service.spreadsheets()
        result = sheet_service.values().get(spreadsheetId=google_spreadsheet_id, range=sheet_name).execute()
        sheet_contents_list = result.get('values', [])
        return sheet_contents_list

    def _get_json_from_sheet_contents_list(self, sheet_contents_list: list, columns_to_export: list, field_name_for_key: str) -> dict:
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
                    key = ""
                    while j < len(row):
                        field_name = field_names_list[j].lower()
                        if field_name in columns_to_export:
                            if field_name == field_name_for_key:
                                key = row[j].strip()
                            else:
                                this_row[field_name] = row[j].strip()
                        j += 1
                    if key:
                        json_node[key] = this_row
        return json_node

    def _save_json_for_this_sheet(self, site_name: str, sheet_name: str, sheet_json: dict):
        """ save json within the "sites" folder, in folder for site_name in file named for sheet_name. """
        sites_dir = self.local_folder + 'sites'
        if not os.path.isdir(sites_dir):
            os.mkdir(sites_dir)
        this_site_dir = sites_dir + '/' + site_name
        if not os.path.isdir(this_site_dir):
            os.mkdir(this_site_dir)
        with open(this_site_dir + "/" + sheet_name.lower() + ".json", "w") as output_file:
            json.dump(sheet_json, output_file, indent=2, ensure_ascii=False)
