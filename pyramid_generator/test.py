import os
import sys
import json

where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am + "/../pipelineutilities/pipelineutilities")
from pyramid import ImageRunner
from pipeline_config import load_pipeline_config

event = {
    'ssm_key_base': '/all/marble-manifest-prod',
    'config-file': '2020-06-22-13:55:36.698390.json',
    'process-bucket': 'marble-manifest-prod-processbucket-kskqchthxshg',
    'ids': [
        'qz20sq9094h'
    ],
    'errors': []
}
config = load_pipeline_config(event)
config['ids'] = ['qz20sq9094h']
runner = ImageRunner(config)
runner.process_images()
