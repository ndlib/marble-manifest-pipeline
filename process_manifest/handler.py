import sys
from pathlib import Path
import os
from iiifCollection import iiifCollection
from ToSchema import ToSchema
from ndJson import ndJson

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
    if 'ssm_key_base' not in event and not event.get('local', False):
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']

    config = get_pipeline_config(event)

    ids = event.get("ids")

    for id in ids:
        inprocess_bucket = InprocessBucket(id, config)

        parent = load_csv_data(id, config)
#        image = load_image_data(id, event)

        # a2s = AthenaToSchema(event, parent, [])
        iiif = iiifCollection(config, parent)
        manifest = iiif.manifest()

        # split the manifests
        for item in sub_manifests(manifest):
            inprocess_bucket.write_sub_manifest(item)

        inprocess_bucket.write_manifest(manifest)

        nd = ndJson(id, config, parent)
        inprocess_bucket.write_nd_json(nd.to_hash())

        schema = ToSchema(id, config, parent)
        inprocess_bucket.write_schema_json(schema.get_json())

    return event


def file_name_from_manifest(manifest, config):
    return manifest.get('id').replace(config['manifest-server-base-url'] + "/", '') + "/index.json"


def sub_manifests(manifest):
    ret = []
    for item in manifest.get('items', []):
        if item.get('type').lower() == 'manifest':
            ret.append(item)

        ret = ret + sub_manifests(item)

    return ret


# python -c 'from handler import *; test()'
def test():
    # import pprint
    # pp = pprint.PrettyPrinter(indent=4)
    event = {}
    event['ids'] = ['item-one-image-embark']
    event['ssm_key_base'] = '/all/new-csv'
    event['process-bucket'] = 'new-csv-processbucket-10dr776tnq9be'
    event['process-bucket-csv-basepath'] = 'csv'
    event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
    event['local'] = True
    run(event, {})
