Before using any code referencing the Google API, we need to store the Google credentials into Parameter Store.

First, a Google service account must be created.  Information on that is here: https://support.google.com/a/answer/7378726?hl=en
Information on using the Google api is here: https://developers.google.com/drive/api/v3/quickstart/python

Once a Google service account is created, export a json file of the credentials and store them in a secure place.

Then, to load these credentials into Parameter Store for use by later lambdas, run the following code in the scripts folder:
  aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
  ./load_json_file_into_ssm_value.sh <SSM_KEY_NAME> <json_filepath_to_load>"
  e.g. ./load_json_file_into_ssm_value.sh /all/marble/google/credentials ../secrets/google_credentials.json

These credentials must be loaded into both the test environment and the production environment.
  e.g. testlibnd and libnd
  
