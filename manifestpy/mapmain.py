def mapMainManifest(readfile, wtype):

    mainOut = '{"@context":"http://schema.org",'
    mainOut += '"@type":"' + wtype + '",'
    if 'label' in readfile.keys():
        mainOut += '"name:":"' + readfile['label'] + '",'
    if 'attribution' in readfile.keys():
        mainOut += '"provider":"' + readfile['attribution'] + '",'
    if 'license' in readfile.keys():
        mainOut += '"license":"' + readfile['license'] + '",'
    if 'thumbnail' in readfile.keys():
        mainOut += '"thumbnailURL":"' + readfile['thumbnail']["@id"] + '",'
    if 'description' in readfile.keys():
        mainOut += '"description":"' + readfile['description'] + '",'
    metl = len(readfile['metadata'])
    i = 0
    while i < metl:
        label = readfile['metadata'][i]['label'].upper()
        value = readfile['metadata'][i]['value']
        if label == 'Keywords'.upper():
            mainOut += '"keywords":"' + value + '",'
        if label == 'Alternate Title'.upper():
            mainOut += '"alternateName":"' + value + '",'
        if label == 'Creation Date'.upper():
            mainOut += '"dateCreated":"' + value + '",'
        if label in ('Accession Number'.upper(), 'Rare Books Identifier'.upper(), 'Identifier'.upper()):
            mainOut += '"identifier":"' + value + '",'
        if label in ('Artist'.upper(), 'Author'.upper(), 'Creator'.upper()):
            mainOut += '"creator":"' + value + '",'
        if label in ('Date'.upper(), 'Dates'.upper()):
            mainOut += '"temporalCoverage":"' + value + '",'
        if label in ('Language of Materials'.upper(), 'Language'.upper()):
            mainOut += '"inLanguage":"' + value + '",'
        if label in ('Media'.upper(), 'Format'.upper()):
            mainOut += '"matierial":"' + value + '",'
        i += 1

    return mainOut
