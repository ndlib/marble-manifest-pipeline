# load_json_file_into_ssm_value.sh

if [ $# -lt 2 ]
  then
    echo "Usage:  ./load_json_file_into_ssm_value.sh <SSM_KEY_NAME> <json_filepath_to_load>"
    echo "  e.g. ./load_json_file_into_ssm_value.sh /all/marble/google/credentials ../secrets/google_credentials.json"
    exit 1
  else
    FileContents=`cat $2`
    aws ssm put-parameter --name "$1" --value "$FileContents" --type="SecureString" --overwrite
fi
