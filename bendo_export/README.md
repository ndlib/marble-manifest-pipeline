# Bendo Export

## Description
This module harvests information from the Bendo API.  Documentation on the Bendo API is here:  https://github.com/ndlib/bendo/blob/master/architecture/howto.md
Note:  We pulled the plug on Bendo export.  It is included here in case we need to pick it up again.

## Setup
Run local_install.sh to set up dependencies

## Process
Export all buckets from bendo.
For each bucket, export file information for all items in the bucket.

## Execute
export SSM_KEY_BASE=/all/<stack_name>  (e.g. new-csv)
export SENTRY_DSN=1234567890
aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
python -c 'from handler import *; test()'
