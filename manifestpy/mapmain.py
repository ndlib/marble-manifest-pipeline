import json
import os


def mapMainManifest(readfile, wtype):
    path = os.path.dirname(os.path.abspath(__file__))
    wfile = os.path.join(path, 'error.txt')
    schemaiiif = os.path.join(path, 'schemaiiif.json')
    writeerror = open(wfile, 'w+')

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
        elif 'metadata' in readfile.keys():
            metl = len(readfile['metadata'])
            i = 0
            while i < metl:
                label = str(readfile['metadata'][i]['label']['true'][0])
                value = str(readfile['metadata'][i]['value']['true'][0])
                if label.upper() == key.upper():
                    if value != '':
                        mainOut.update({val: value})
                    else:
                        error.append(str(key) + ' has no value assigned \n')
                i += 1
    writeerror.write(''.join(error))
    return mainOut
