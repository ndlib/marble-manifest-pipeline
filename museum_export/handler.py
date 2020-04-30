# handler.py
""" Module to launch application """

import _set_path  # noqa
import os
from pathlib import Path
from process_web_kiosk_json_metadata import processWebKioskJsonMetadata  # noqa: E402
from pipelineutilities.pipeline_config import setup_pipeline_config, load_config_ssm  # noqa: E402
import sentry_sdk  # noqa: E402
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration  # noqa: E402


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

    if config:
        mode = event.get("mode", "full")
        if mode not in ["full", "incremental", "ids"]:
            mode = "full"
        jsonWebKioskClass = processWebKioskJsonMetadata(config, event)
        composite_json = jsonWebKioskClass.get_composite_json_metadata(mode)
        if composite_json:
            jsonWebKioskClass.process_composite_json_metadata(composite_json)
        else:
            print('No JSON to process')
    else:
        raise SystemExit('No configuration defined.  Unable to continue.')
    return event


def _suplement_event(event):
    if 'ssm_key_base' not in event and 'SSM_KEY_BASE' in os.environ:
        event['ssm_key_base'] = os.environ['SSM_KEY_BASE']
    if 'local-path' not in event:
        event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
    return


# setup:
# cd museum_export
# aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
# export SSM_KEY_BASE=/all/new-csv
# python -c 'from handler import *; test()'
# python 'run_all_tests.py'
def test():
    """ test exection """
    event = {}
    event["mode"] = "full"
    # event["mode"] = "ids"
    event["ids"] = ["1990.005.001", "1990.005.001.a", "1990.005.001.b"]  # parent / child objects
    # event["ids"] = ["1979.032.003"]  # objects with special characters to strip
    # event["ids"] = ["L1986.032.002"]  # objects with missing Google images on Google Drive
    # event["ids"] = ["2017.039.005", "1986.052.007.005", "1978.062.002.003"]  # Objects with hidden parents
    # Test these temp IDs:  IL2019.006.002, IL1992.065.004, L1986.032.002, AA1966.031
    print(run(event, {}))
