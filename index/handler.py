import os
# import boto3
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

# from create_json_items_from_embark_xml import create_json_items_from_embark_xml
from index_manifest import index_manifest


def run(event, context):
    config = event.get("config")
    config["local-dir"] = "/tmp/index/"
    #index_manifest(event.get("id"), config)
    # still need to write to S3 bucket and copy to Aleph server
    return event


# python -c 'from handler import *; test()'
def test():
    data = {"id": "ils-000909885"}
    run(data, {})
