# Aleph Export

## Description
This module harvests MARC records from Aleph into a CSV format of our own design.  Only records with 956 records will be harvested.
This requires additional work to create a required MARC file on our Aleph server:
  1.  Populating a 956 field in MARC records of interest
  2.  Creating a query on the Aleph server which harvests all MARC records containing our populated 956 field
  3.  Executing that query nightly, and saving the resulting output in a .mrc file on the server
Our process downloads and processes that .mrc file

## Setup
Run local_install.sh to set up dependencies

## Execute
export SSM_KEY_BASE=/all/new-csv
export SENTRY_DSN=1234567890
aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
python -c 'from handler import *; test()'
