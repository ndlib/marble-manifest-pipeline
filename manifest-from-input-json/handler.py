import os
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

from processJson import processJson


def run(event, context):
    processSet = processJson()
    print("yo")
    print(event.get("data")['unique-identifier'])
    
    processSet._set_file_data("unused", "unused", "unused")
    processSet._create_manifest_json(event.get("data"))
    event.update( { "manifest": processSet.result_json })

    #print resulting json to STDOUT
    return event
