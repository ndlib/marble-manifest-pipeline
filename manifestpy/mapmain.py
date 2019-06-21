def mapMainManifest(readfile, wtype):

    mainOut = '{"@context":"http://schema.org",'
    mainOut += '"@type":"' + wtype + '",'
    if 'label' in readfile.keys(): mainOut += '"name:":"' + readfile['label'] + '",'
    if 'attribution' in readfile.keys(): mainOut += '"provider":"' + readfile['attribution'] + '",'
    if 'license' in readfile.keys(): mainOut += '"license":"' + readfile['license'] + '",'
    if 'thumbnail' in readfile.keys(): mainOut += '"thumbnailURL":"' + readfile['thumbnail']["@id"] + '",'
    if 'description' in readfile.keys(): mainOut += '"description":"' + readfile['description'] + '",'
    metl = len(readfile['metadata'])
    i = 0
    while i < metl:
        label = readfile['metadata'][i]['label']
        value = readfile['metadata'][i]['value']
         if label == 'Keywords': mainOut += '"keywords":"' + value + '",'
         if label == 'Alternate Title': mainOut += '"alternateName":"' + value + '",'
         if label == 'Creation Date': mainOut += '"dateCreated":"' + value + '",'
         if label in ('Accession Number', 'Rare Books Identifier', 'Identifier'): mainOut += '"identifier":"' + value + '",'
         if label in ('Artist','Author','Creator'): mainOut += '"creator":"' + value + '",'
         if label in ('Date', 'Dates'): mainOut += '"temporalCoverage":"' + value + '",'
         if label in ('Language of Materials', 'Language'): mainOut += '"inLanguage":"' + value +'",'
         if label in ('Media', 'Format'): mainOut += '"matierial":"' + value + '",'
         i += 1

    return mainOut
