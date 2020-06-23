# Import Google Sheet

## Description
This module harvests information from a google sheet which describes metadata to display on a website.

## Setup
Run local_install.sh to set up dependencies
In order to retrieve information from a Google sheet, you will need to set up a Google service account.
That Google service account will need to have Google sheet access enabled.
You will also need to save the Google credentials into parameter store.
Finally, you will need to create a json file for each site.  This json file will need to contain these fields:
  googleSpreadsheetId
  fieldForKey
  columnsToExport
Each spreadsheet to harvest, will need to be shared with the Google service account.


## Process
For each site to process, add the site name (e.g. "marble") to the event["sites"] array.
For each site in event["sites"]:
  Information is retrieved from the appropriate json file.
  This information is used to harvest metadata from a Google sheet, which will later be used when creating manifests and when building web sites.
  This information is stored in the manifest bucket and locally in the process_manifest folder.


## Execute
export SSM_KEY_BASE=/all/<stack_name>  (e.g. new-csv)
export SENTRY_DSN=1234567890
python -c 'from handler import *; test()'
