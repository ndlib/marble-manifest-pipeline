import json
import os


def mapMainManifest(readfile, wtype):
    path = os.path.dirname(os.path.abspath(__file__))
    schemaiiif = os.path.join(path, 'schemaiiif.json')

    with open(schemaiiif) as json_file:
        fieldmap = json.load(json_file)
    mainOut = {
        "@context": "http://schema.org",
        "@type": wtype,
    }
    error = []
    for key, val in fieldmap.items():
        if key in readfile.keys():
            if readfile[key] != '':
                mainOut.update({val: readfile[key]})
            else:
                error += readfile[key] + ' has no value assigned \n'

    return mainOut
