import os
import sys
# import json

where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am + "/../pipelineutilities/pipelineutilities")
from pyramid import ImageRunner  # noqa: E502
from pipeline_config import load_pipeline_config  # noqa: E502

# try:
if True:
    # event = json.loads("{\"config-file\": \"2020-04-28-12:43:07.205855.json\", \"process-bucket\": \"marble-manifest-prod-processbucket-kskqchthxshg\", \"errors\": [], \"local\": false}")
    event = {
        "config-file": "2020-05-13-18:18:07.449923.json",
        "process-bucket": "marble-manifest-prod-processbucket-kskqchthxshg",
        "process-bucket-data-basepath": "json",
        "errors": [],
        "local": False,
        "ecs-args": [
            "{\"config-file\": \"2020-05-13-18:18:07.449923.json\", \"process-bucket\": \"marble-manifest-prod-processbucket-kskqchthxshg\", \"errors\": [], \"local\": false}"
        ]
    }

    config = load_pipeline_config(event)
    config['ids'] = ['002208263']
    print(config['ids'])

    runner = ImageRunner(config)
    runner.process_images()
# except Exception as e:
#     print(e)
