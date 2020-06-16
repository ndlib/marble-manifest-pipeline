import os
import sys
import json

where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am + "/../pipelineutilities/pipelineutilities")
from pyramid import ImageRunner
from pipeline_config import load_pipeline_config

try:
    event = json.loads("{\"config-file\": \"2020-04-28-12:43:07.205855.json\", \"process-bucket\": \"marble-manifest-prod-processbucket-kskqchthxshg\", \"errors\": [], \"local\": false}")
    config = load_pipeline_config(event)
    config['ids'] = ['002207293']
    print(config['ids'])

    runner = ImageRunner(config)
    runner.process_images()
except Exception as e:
    print(e)
