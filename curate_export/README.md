# Aleph Export

## Description
This module harvests information from a CurateND batch ingest CSV into a CSV format of our own design.
Eventually, once the CurateND API is extended to allow us to query all information associated with a work, we would like to be able to populate event["ids"] with Curate ids, and then harvest those.
For now, we will accept event["files"], which we will process.

## Setup
Run local_install.sh to set up dependencies

## Process
For each file in event["files"]:
Read all available information

## Execute
export SSM_KEY_BASE=/all/<stack_name>  (e.g. new-csv)
export SENTRY_DSN=1234567890
aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
python -c 'from handler import *; test()'

## Run Blueprints
export S3_BUCKET=testlibnd-cf
aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
./local-deploy.sh <stack_name> ../marble-blueprints
