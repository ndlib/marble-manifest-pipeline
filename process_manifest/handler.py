import _set_path  # noqa
import os
from iiifManifest import iiifManifest
from MetadataMappings import MetadataMappings
from ToSchema import ToSchema
from ndJson import ndJson
from pipelineutilities.csv_collection import load_csv_data
from pipelineutilities.pipeline_config import load_pipeline_config, cache_pipeline_config
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
    config = load_pipeline_config(event)
    ids = config.get("ids")

    quittime = datetime.utcnow() + timedelta(seconds=config['seconds-to-allow-for-processing'])
    event['process_manifest_complete'] = False

    if 'processed_ids' not in config:
        config['processed_ids'] = []

    if 'process_manifest_run_number' not in config:
        config['process_manifest_run_number'] = 0
    config['process_manifest_run_number'] = config['process_manifest_run_number'] + 1

    if config['process_manifest_run_number'] > 5:
        raise Exception("Too many executions")

    for id in ids:
        if id not in config['processed_ids']:
            inprocess_bucket = InprocessBucket(id, config)

            # move the nd json into the process bucket.
            try:
                inprocess_bucket.write_nd_json()

                parent = load_csv_data(id, config)

                mapping = MetadataMappings(parent)
                iiif = iiifManifest(config, parent, mapping)
                manifest = iiif.manifest()

                # split the manifests
                for item in sub_manifests(manifest):
                    inprocess_bucket.write_sub_manifest(item)

                inprocess_bucket.write_manifest(manifest)

                schema = ToSchema(id, config, parent)
                inprocess_bucket.write_schema_json(schema.get_json())

                config['processed_ids'].append(id)

            except Exception:
                print("error on {}" % (id))

        if quittime <= datetime.utcnow():
            break

    if len(config['ids']) == len(config['processed_ids']):
        event['process_manifest_complete'] = True

    cache_pipeline_config(config, event)

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
    event = {
        'ssm_key_base': '/all/marble-manifest-prod',
        'config-file': '2020-04-15-13:10:11.652565.json',
        'process-bucket': 'new-csv-processbucket-10dr776tnq9be',
        'ids': [
            '1952.019'
        ],
        'local': True,
        'local-path': '../example/',
        'errors': []
    }
    run(event, {})
