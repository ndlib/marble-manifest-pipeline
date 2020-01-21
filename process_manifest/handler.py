import sys
from pathlib import Path
import os
from iiifCollection import iiifCollection

where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am)
sys.path.append(where_i_am + "/dependencies")

from dependencies.pipelineutilities.csv_collection import load_csv_data
from dependencies.pipelineutilities.pipeline_config import get_pipeline_config
from dependencies.pipelineutilities.s3_helpers import InprocessBucket

import dependencies.sentry_sdk
from dependencies.sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
# sentry_sdk.init(
#    dsn=os.environ['SENTRY_DSN'],
#    integrations=[AwsLambdaIntegration()]
# )


def run(event, context):
    if 'ssm_key_base' not in event:
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']

    event = get_pipeline_config(event)

    ids = event.get("ids")

    for id in ids:
        inprocess_bucket = InprocessBucket(id, event)

        parent = load_csv_data(id, event)
        # a2s = AthenaToSchema(event, parent, [])
        iiif = iiifCollection(id, event, parent)
        inprocess_bucket.write_manifest(iiif.manifest())

    return event


# python -c 'from handler import *; test()'
def test():
    # import pprint
    # pp = pprint.PrettyPrinter(indent=4)
    event = {}
    event['ids'] = ['parsons', '1976.057']
    event['ssm_key_base'] = '/all/new-csv'
    event['csv-data-files-bucket'] = 'marble-manifest-test-processbucket-19w6raq5mndlo'
    event['csv-data-files-basepath'] = 'archives-space-csv-files'
    event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
    event['local'] = True
    run(event, {})
