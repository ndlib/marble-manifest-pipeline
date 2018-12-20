import os
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

from processJson import processJson


def run(event, context):
    processSet = processJson()
    processSet._set_file_data("unused", "unused", "unused")
    processSet._create_manifest_json(event.get("data"))
    processSet.dumpManifest()

    event.update( { "manifest": processSet.result_json })

    #print resulting json to STDOUT
    return event

    # python -c 'from handler import *; test()'
def test():
    data = {
      "id": "example",
      "data": {
        "errors": [],
        "creator": "creator@email.com",
        "metadata": [
          {
            "label": "Title",
            "value": "Wunder der Verenbung"
          },
          {
            "label": "Author(s)",
            "value": "Bolle, Fritz"
          },
          {
            "label": "Publication date",
            "value": "[1951]"
          },
          {
            "label": "Attribution",
            "value": "Welcome Library<br/>License: CC-BY-NC"
          }
        ],
        "sequences": [
          {
            "pages": [
              {
                "file": "009_output",
                "label": "009"
              },
              {
                "file": "046_output.tif",
                "label": "046"
              },
              {
                "file": "2018_009.jpg",
                "label": "2018 009"
              },
              {
                "file": "2018_049_009.jpg",
                "label": "2018 049 009"
              }
            ],
            "viewingHint": "paged",
            "label": "sequency"
          }
        ],
        "label": "Label",
        "description": "Description",
        "attribution": "attribution",
        "rights": "rights",
        "unique-identifier": "2018_example_001",
        "iiif-server": "https://image-server.library.nd.edu:8182/iiif/2",
        "manifest-base-url": "https://manifest.nd.edu/"
      }
    }
    print(run(data, {}))
