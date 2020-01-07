# get_config.py
""" Get configuration for this project
    Requires environment variable called SSM_KEY_BASE (for pipeline get_config),
    and one called SSM_MARBLE_DATA_PROCESSING_KEY_BASE (for this get_config)
    which will hold the paths to parameter store to retrieve additional config values.

    Because there are secrets stored in parameter store, these cannot be passed between step function lambdas,
    therefore, we will need to retrieve the configuration in each step function. """

import os
import sys
import manifest_utils as mu
where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am)


def get_config():
    config = {}
    hours_threshold = 24  # default to 24 hours if not specified
    if 'HOURS_THRESHOLD' in os.environ:
        hours_threshold = os.environ['HOURS_THRESHOLD']
    if 'SSM_MARBLE_DATA_PROCESSING_KEY_BASE' not in os.environ:
        print('An environment variable called SSM_MARBLE_DATA_PROCESSING_KEY_BASE is required for Parameter Store.'
              + ' Consider adding something like: '
              + ' export SSM_MARBLE_DATA_PROCESSING_KEY_BASE=/all/marble-data-processing/test')
    elif 'SSM_KEY_BASE' not in os.environ:
        print('Unable to retrieve parameter store values from SSM_KEY_BASE of : ' + os.environ['SSM_KEY_BASE']
              + ' Consider adding something like:  export SSM_KEY_BASE=/all/manifest-pipeline-v3')
    else:
        config = {
            # This item is duplicated from marble-manfiest-pipeline/init/handler.py
            "process-bucket-read-basepath": 'process',
            #
            "hours-threshold": hours_threshold,
            "google": {
                "credentials": {},
                "museum": {
                    "metadata": {},
                    "image": {}
                },
                "library": {
                    "metadata": {},
                    "image": {}
                }
            },
            "sentry": {},
            "embark": {},
            "museum": {}
        }
        config = _get_parameter_store_config(config, os.environ['SSM_MARBLE_DATA_PROCESSING_KEY_BASE'])
        config = _get_parameter_store_config(config, os.environ['SSM_KEY_BASE'])
    return config


def _get_parameter_store_config(config, ssm_path):
    for ps in mu.ssm_get_params_by_path(ssm_path):
        value = ps['Value']
        key = ps['Name'].replace(ssm_path, '')
        _add_key_value_pair_to_config(config, key, value)
    return config


def _add_key_value_pair_to_config(config, key, value):
    if 'google/credentials/' in key:
        key = key.replace('google/credentials/', '')
        if key == 'private_key':
            value = value + "\n"  # this is to correct Parameter Store's stripping trailing \n for certificate
        config['google']['credentials'][key] = value
    elif 'google/museum/metadata/' in key:
        key = key.replace('google/museum/metadata/', '')
        config['google']['museum']['metadata'][key] = value
    elif 'google/museum/image/' in key:
        key = key.replace('google/museum/image/', '')
        config['google']['museum']['image'][key] = value
    elif 'google/library/metadata/' in key:
        key = key.replace('google/library/metadata/', '')
        config['google']['library']['metadata'][key] = value
    elif 'google/library/image/' in key:
        key = key.replace('google/library/image/', '')
        config['google']['library']['image'][key] = value
    elif 'sentry/' in key:
        key = key.replace('sentry/', '')
        config['sentry'][key] = value
    elif 'embark/' in key:
        key = key.replace('embark/', '')
        config['embark'][key] = value
    elif 'museum/' in key:
        key = key.replace('museum/', '')
        config['museum'][key] = value
    else:
        config[key] = value
