import json


def mapMainManifest(readfile, wtype):
    with open('schemaiiif.json') as json_file:
        fieldmap = json.load(json_file)
    mainOut = {
        "@context": "http://schema.org",
        "@type": wtype,
    }
    for key, val in fieldmap.items():
        if key in readfile.keys():
            mainOut.update({val: readfile[key]})
        elif readfile['metadata']:
            metl = len(readfile['metadata'])
            i = 0
            while i < metl:
                label = readfile['metadata'][i]['label']['true'][0]
                value = readfile['metadata'][i]['value']['true'][0]
                if value != '':
                    if label.upper() == key.upper():
                        mainOut.update({val: value})
                i += 1
    return mainOut
