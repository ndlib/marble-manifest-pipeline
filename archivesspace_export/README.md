# Harvest EADs

## Description
This module harvests OAI EADs from ArchivesSpace into a CSV format of our own design.  Only EAD records with corresponding DAO (digital asset object) records are harvested.

Various modes exist for this harvest:
full - Export all EADs.
    When this is run, we generate a dictionary of EAD ids and the associated ArchivesSpace resource.  These are stored in a file called "ead_to_resource_dictionary.json"
incremental - Export all EADs which have changed recently (the time frame is defined in the config variable: "hours-threshold-for-incremental-harvest")
known - Export all known EADs as identified the last time we ran a full export.
ids - Export all ids in a list passed.  E.g.  ids = ['MSNEA8011_EAD']
identifiers - Export all identifiers in a list passed:  E.g.  ids = ["oai:und//repositories/3/resources/1644"]

Each of these processes are time limited, and will only run for config["seconds-to-allow-for-processing"] seconds.
If the process is not yet complete, event will contain two entries: eadHarvestComplete (boolean)
        and resumptionToken.  If eadHarvestComplete is false, the harvest needs to be re-run, and will resume with the record indicated by the resumptionToken.

## Setup
Run local_install.sh to set up dependencies

## Execute
export MODE=<appropriate mode>  e.g. export MODE=full
export SSM_KEY_BASE=/all/manifest-pipeline-v3
export SENTRY_DSN=1234567890
aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
python -c 'from handler import *; test()'

## To run blueprints:
export S3_BUCKET=<s3 bucket>  e.g. testlibnd-cf
aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
./local-deploy.sh new-csv ../marble-blueprints
