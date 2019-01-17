import os
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

from processCsv import processCsv

def run(event, context):
    csvSet = processCsv(event.get("id"), os.environ['PROCESS_BUCKET'], os.environ['MANIFEST_BUCKET'], os.environ['IMAGE_BUCKET'], os.environ['MANIFEST_URL'], os.environ['IMAGE_SERVER_URL'])
    if not csvSet.verifyCsvExist():
        raise Exception(csvSet.error)

    csvSet.buildJson()
    csvSet.writeEventData({ "data": csvSet.result_json })
    event.update({ "event-config":
        {
            "process-bucket": csvSet.config['process-bucket'],
            "process-bucket-read-basepath": csvSet.config['process-bucket-read-basepath'],
            "process-bucket-write-basepath": csvSet.config['process-bucket-write-basepath'],
            "event-file": csvSet.config['event-file']
        }
    })
    return event

# python -c 'from handler import *; test()'
def test():
    data = { "id": "example"}
    print(run(data, {}))
