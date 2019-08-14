import json


def mapMetsManifest(readfile, wtype):
    with open('schemamets.json') as json_file:
        fieldmap = json.load(json_file)
    xmlBase = readfile['mets:mets']['mets:dmdSec']
    xmlData = {}
    for i in xmlBase:
        if i['@ID'] == 'DSC_01_SNITE':
            xmlData.update(i['mets:mdWrap']['mets:xmlData'])
        if i['@ID'] == 'DSC_02_SNITE':
            vraBase = i['mets:mdWrap']['mets:xmlData']['vracore:work']
            xmlData.update(vraBase['vracore:materialSet'])
            xmlData.update(vraBase['vracore:dateSet'])
            xmlData.update(vraBase['vracore:measurementsSet'])

    mainOut = {
        '@context': 'http://schema.org',
        'type': wtype,
    }
    for key, val in fieldmap.items():
        if val in xmlData.keys():
            mainOut[key] = json.dumps(xmlData[val]).replace('\"', '')

    return mainOut
