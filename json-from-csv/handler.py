import os
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

from processCsv import processCsv

def run(event, context):
    event.update({ "config":
        {
            "image-server-base-url": 'https://' + os.environ['IMAGE_SERVER_URL'] + ':8182/iiif/2',
            "manifest-server-base-url": 'https://' + os.environ['MANIFEST_URL'],
            "process-bucket": os.environ['PROCESS_BUCKET'],
            "process-bucket-read-basepath": 'process',
            "process-bucket-write-basepath": 'finished',
            "image-server-bucket": os.environ['IMAGE_BUCKET'],
            "image-server-bucket-basepath": '',
            "manifest-server-bucket": os.environ['MANIFEST_BUCKET'],
            "manifest-server-bucket-basepath": '',
            "sequence-csv": 'sequence.csv',
            "main-csv": 'main.csv',
            "canvas-default-height": 2000,
            "canvas-default-width": 2000,
            "notify-on-finished": "jhartzle@nd.edu",
            "event-file": "event.json"
        }
    })
    csvSet = processCsv(event.get("id"), event.get("config"))
    if not csvSet.verifyCsvExist():
        raise Exception(csvSet.error)

    csvSet.buildJson()
    csvSet.writeEventData({ "data": csvSet.result_json })
    return event

# python -c 'from handler import *; test()'
def test():
    data = { "id": "example"}
    print(run(data, {}))
