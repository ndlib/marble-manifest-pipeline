import json


def mapMetsManifest(readfile, wtype):
    xmlData = readfile['mets:mets']['mets:dmdSec'][0]['mets:mdWrap']['mets:xmlData']
    xmlVRA = readfile['mets:mets']['mets:dmdSec'][1]['mets:mdWrap']['mets:xmlData']['vracore:work']
    mainOut = {
        '@context': 'http://schema.org',
        'type': wtype,
        'name': json.dumps(xmlData['dcterms:title']),
        'provider': json.dumps(xmlData['dcterms:publisher']),
        'license': json.dumps(xmlData['dcterms:rights']),
        "description": json.dumps(xmlData['dcterms:provenance']),
        "conditionOfAccess": json.dumps(xmlData['dcterms:accessRights']),
        "identifier": json.dumps(xmlData['dcterms:identifier']),
        "creator": json.dumps(xmlData['dcterms:creator']),
        "temporalCoverage": json.dumps(xmlData['dcterms:created']),
        "inLanguage": json.dumps(xmlVRA['vracore:agentSet']['vracore:culture']),
        "material": json.dumps(xmlVRA['vracore:materialSet']['vracore:display'])
    }

    for k, v in mainOut.items():
        mainOut[k] = mainOut[k].strip('\"')
    if 'dcterms:subject' in xmlData.keys():
        kw = json.dumps(xmlData['dcterms:subject'])
        kw = kw.replace('\"', '')
        kw = kw.strip('[')
        kw = kw.strip(']')
        kw = kw.split(',')
        print kw
        mainOut["keywords"] = kw

    return mainOut
