import os
# import boto3
import sys
import json
from index_manifest import index_manifest
sys.path.append(os.path.dirname(os.path.realpath(__file__)))


def run(event, context):
    config = event.get("config")
    config["local-dir"] = "/tmp/index/"
    index_manifest(event.get("id"), config)
    return event


# python -c 'from handler import *; test()'
def test():
    with open("../example/example-input.json", 'r') as input_source:
        data = json.load(input_source)
    input_source.close()
    data = {
      "id": "example",
      "config": data["config"],
      "manifestData": data
    }
    print(run(data, {}))
