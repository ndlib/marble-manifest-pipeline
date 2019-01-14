import os, json
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

from processJson import processJson

def run(event, context):
    processSet = processJson(event.get("id"), event.get("data"))
    processSet._create_manifest_json()
    # if there are errors add them in

    processSet.dumpManifest()

    #print resulting json to STDOUT
    return event

# python -c 'from handler import *; test()'
def test():
    with open("../example/example-input.json", 'r') as input_source:
        data = json.load(input_source)
    input_source.close()

    data = {
      "id": "example",
      "data": data
    }
    print(run(data, {}))
