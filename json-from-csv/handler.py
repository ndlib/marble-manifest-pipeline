import os
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

from processCsv import processCsv

def run(event, context):
    csvSet = processCsv(event.get("id"), os.environ['PROCESS_BUCKET'], os.environ['MANIFEST_BUCKET'], os.environ['IMAGE_BUCKET'], os.environ['MANIFEST_URL'])
    if not csvSet.verifyCsvExist():
        raise Exception(csvSet.error)

    csvSet.buildJson()
    event.update( { "data": csvSet.result_json })
    #print resulting json to STDOUT
    return event

# python -c 'from handler import *; test()'
def test():
    data = { "id": "example"}
    print(run(data, {}))
