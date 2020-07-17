# handler.py
""" Module to launch application """

import _set_path  # noqa
import os
from pathlib import Path
from datetime import datetime, timedelta
import sentry_sdk
import io
import json
from process_web_kiosk_json_metadata import processWebKioskJsonMetadata
from pipelineutilities.pipeline_config import setup_pipeline_config, load_config_ssm
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
from clean_up_composite_json import CleanUpCompositeJson
from s3_helpers import write_s3_json, read_s3_json, s3_file_exists, delete_s3_key


if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(dsn=os.environ['SENTRY_DSN'], integrations=[AwsLambdaIntegration()])


def run(event, context):
    """ run the process to retrieve and process web kiosk metadata """
    _suplement_event(event)
    config = setup_pipeline_config(event)
    google_config = load_config_ssm(config['google_keys_ssm_base'])
    config.update(google_config)
    museum_config = load_config_ssm(config['museum_keys_ssm_base'])
    config.update(museum_config)
    time_to_break = datetime.now() + timedelta(seconds=config['seconds-to-allow-for-processing'])
    print("Will break after ", time_to_break)

    mode = event.get("mode", "full")
    if mode not in ["full", "incremental", "ids"]:
        mode = "full"
    jsonWebKioskClass = processWebKioskJsonMetadata(config, event, time_to_break)
    if event["museum_execution_count"] == 1:
        composite_json = jsonWebKioskClass.get_composite_json_metadata(mode)
        museum_image_metadata = jsonWebKioskClass.find_images_for_composite_json_metadata(composite_json)
        composite_json = CleanUpCompositeJson(composite_json).cleaned_up_content
        write_s3_json(config['process-bucket'], 'museum_composite_metadata.json', composite_json)
        write_s3_json(config['process-bucket'], 'museum_image_metadata.json', museum_image_metadata)
    else:
        composite_json = read_s3_json(config['process-bucket'], 'museum_composite_metadata.json')
        museum_image_metadata = read_s3_json(config['process-bucket'], 'museum_image_metadata.json')

    if composite_json:
        jsonWebKioskClass.process_composite_json_metadata(composite_json, museum_image_metadata)
        event['museumHarvestComplete'] = _done_processing(composite_json)
    else:
        print('No JSON to process')

    if event["museum_execution_count"] >= event["maximum_museum_executions"]:
        event['museumHarvestComplete'] = True
    if event['museumHarvestComplete']:
        if s3_file_exists(config['process-bucket'], 'museum_composite_metadata.json'):
            delete_s3_key(config['process-bucket'], 'museum_composite_metadata.json')
        if s3_file_exists(config['process-bucket'], 'museum_image_metadata.json'):
            delete_s3_key(config['process-bucket'], 'museum_image_metadata.json')
    elif composite_json:
        write_s3_json(config['process-bucket'], 'museum_composite_metadata.json', composite_json)

    return event


def _done_processing(composite_json):
    done_flag = True
    if "objects" in composite_json:
        for _key, value in composite_json["objects"].items():
            if not value.get("recordProcessedFlag", False):
                done_flag = False
    return done_flag


def _suplement_event(event):
    if 'ssm_key_base' not in event and 'SSM_KEY_BASE' in os.environ:
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']
    if 'local-path' not in event:
        event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
    event['museumHarvestComplete'] = event.get('museumHarvestComplete', False)
    event["museum_execution_count"] = event.get("museum_execution_count", 0) + 1
    event["maximum_museum_executions"] = event.get("maximum_museum_executions", 10)
    return


# setup:
# export SSM_KEY_BASE=/all/new-csv
# aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
# python -c 'from handler import *; test()'
# python 'run_all_tests.py'
def test():
    """ test exection """
    filename = 'event.json'
    if os.path.exists(filename):
        with io.open(filename, 'r', encoding='utf-8') as json_file:
            event = json.load(json_file)
    else:
        event = {}
        event["local"] = False
        event["mode"] = "full"
        # event['seconds-to-allow-for-processing'] = 60
        # event["mode"] = "ids"
        # event['ids'] = ['1994.042', '1994.042.a', '1994.042.b']  # , '1990.005.001']
        # event["ids"] = ["1990.005.001", "1990.005.001.a", "1990.005.001.b"]  # parent / child objects
        # event["export_all_files_flag"] = True
        # event["ids"] = ["1979.032.003"]  # objects with special characters to strip
        # event["ids"] = ["L1986.032.002"]  # objects with missing Google images on Google Drive
        # event["ids"] = ["2017.039.005", "1986.052.007.005", "1978.062.002.003"]  # Objects with hidden parents
        # Test these temp IDs:  IL2019.006.002, IL1992.065.004, L1986.032.002, AA1966.031
    event = run(event, {})

    if not event['museumHarvestComplete']:
        with open('event.json', 'w') as f:
            json.dump(event, f, indent=2)
    else:
        try:
            os.remove('event.json')
        except FileNotFoundError:
            pass
    print(event)
