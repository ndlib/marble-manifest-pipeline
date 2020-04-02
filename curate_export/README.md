# Aleph Export

## Description
This module harvests information from the CurateND API. We harvest collection information listed in event["ids"].
If the CurateND API is too slow, we may need to revert to harvesting from CSV files that were used for Curate batch ingest.

## Setup
Run local_install.sh to set up dependencies

## Process
For each file in event["ids"]:
Read all available information on that item and any children using the Curate API.
For each of the files referenced, access the Bendo API to retrieve md5checksum information.

## Execute
export SSM_KEY_BASE=/all/<stack_name>  (e.g. new-csv)
export SENTRY_DSN=1234567890
aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
python -c 'from handler import *; test()'

## Run Blueprints
export S3_BUCKET=testlibnd-cf
aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
./local-deploy.sh <stack_name> ../marble-blueprints
