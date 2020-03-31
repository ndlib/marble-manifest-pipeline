import _set_path  # noqa
from pathlib import Path
import os
from iiifManifest import iiifManifest
from MetadataMappings import MetadataMappings
from ToSchema import ToSchema
from ndJson import ndJson
from pipelineutilities.csv_collection import load_csv_data
from pipelineutilities.pipeline_config import get_pipeline_config
from pipelineutilities.s3_helpers import InprocessBucket
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
from datetime import datetime, timedelta


if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(
        dsn=os.environ['SENTRY_DSN'],
        integrations=[AwsLambdaIntegration()]
    )


def run(event, context):
    if 'ssm_key_base' not in event and not event.get('local', False):
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']

    config = get_pipeline_config(event)
    ids = event.get("ids")

    quittime = datetime.utcnow() + timedelta(seconds=config['seconds-to-allow-for-processing'])
    event['process_manifest_complete'] = False

    if 'processed_ids' not in event:
        event['processed_ids'] = []

    if 'process_manifest_run_number' not in event:
        event['process_manifest_run_number'] = 0
    event['process_manifest_run_number'] = event['process_manifest_run_number'] + 1

    if event['process_manifest_run_number'] > 5:
        raise Exception("Too many executions")

    for id in ids:
        if id not in event['processed_ids']:
            inprocess_bucket = InprocessBucket(id, config)

            parent = load_csv_data(id, config)

            mapping = MetadataMappings(parent)
            iiif = iiifManifest(config, parent, mapping)
            manifest = iiif.manifest()

            # split the manifests
            for item in sub_manifests(manifest):
                inprocess_bucket.write_sub_manifest(item)

            inprocess_bucket.write_manifest(manifest)

            nd = ndJson(id, config, parent)
            inprocess_bucket.write_nd_json(nd.to_hash())

            schema = ToSchema(id, config, parent)
            inprocess_bucket.write_schema_json(schema.get_json())

            event['processed_ids'].append(id)

        if quittime <= datetime.utcnow():
            break

    if len(config['ids']) == len(event['processed_ids']):
        event['process_manifest_complete'] = True

    return event


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
    event['ids'] = ['1988.012']
    event['ssm_key_base'] = '/all/new-csv'
    event['process-bucket'] = 'new-csv-processbucket-10dr776tnq9be'
    event['process-bucket-csv-basepath'] = 'csv'
    event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
    event['local'] = False
    run(event, {})
