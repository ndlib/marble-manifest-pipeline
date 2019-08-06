import json


def mapMetsManifest(readfile, wtype):
    xmlBase = readfile['mets:mets']['mets:dmdSec']
    for i in xmlBase:
        if i['@ID'] == 'DSC_01_SNITE':
            xmlDC = i['mets:mdWrap']['mets:xmlData']
        if i['@ID'] == 'DSC_02_SNITE':
            xmlVRA = i['mets:mdWrap']['mets:xmlData']['vracore:work']
    mainOut = {
        '@context': 'http://schema.org',
        'type': wtype,
        'name': json.dumps(xmlDC['dcterms:title']),
        'provider': json.dumps(xmlDC['dcterms:publisher']),
        'license': json.dumps(xmlDC['dcterms:rights']),
        "description": json.dumps(xmlDC['dcterms:provenance']),
        "alternateName": json.dumps(xmlDC['dcterms:accessRights']),
        "identifier": json.dumps(xmlDC['dcterms:identifier']),
        "creator": json.dumps(xmlDC['dcterms:creator']),
        "temporalCoverage": json.dumps(xmlDC['dcterms:created']),
        "material": json.dumps(xmlVRA['vracore:materialSet']['vracore:display'])
    }
    for k, v in mainOut.items():
        mainOut[k] = mainOut[k].strip('\"')
    if 'dcterms:subject' in xmlDC.keys():
        kw = json.dumps(xmlDC['dcterms:subject'])
        kw = kw.replace('\"', '')
        kw = kw.strip('[')
        kw = kw.strip(']')
        kw = kw.split(',')
        mainOut["keywords"] = kw

    return mainOut
