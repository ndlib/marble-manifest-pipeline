import os
import sys
import json

where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am + "/../pipelineutilities/pipelineutilities")
from pyramid import ImageRunner
from pipeline_config import load_pipeline_config


event = json.loads( "{\"config-file\": \"2020-04-23-13:03:42.346710.json\", \"process-bucket\": \"marble-manifest-prod-processbucket-kskqchthxshg\", \"errors\": [], \"local\": false}")
config = load_pipeline_config(event)
runner = ImageRunner(config)
runner.process_images()
