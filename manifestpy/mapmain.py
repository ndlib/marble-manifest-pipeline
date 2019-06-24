def mapMainManifest(readfile, wtype):

    mainOut = {
        "@context": "http://schema.org",
        "@type": wtype,
    }
    if 'label' in readfile.keys():
        mainOut.update({"name": readfile['label']})
    if 'attribution' in readfile.keys():
        mainOut.update({"provider": readfile['attribution']})
    if 'license' in readfile.keys():
        mainOut.update({"license": readfile['license']})
    if 'thumbnail' in readfile.keys():
        mainOut.update({"thumbnailURL": readfile['thumbnail']['@id']})
    if 'description' in readfile.keys():
        mainOut.update({"description": readfile['description']})
    metl = len(readfile['metadata'])
    i = 0
    while i < metl:
        label = readfile['metadata'][i]['label'].upper()
        value = readfile['metadata'][i]['value']
        if label == 'Keywords'.upper():
            mainOut.update({"keywords": value})
        if label == 'Alternate Title'.upper():
            mainOut.update({"alternateName": value})
        if label == 'Creation Date'.upper():
            mainOut.update({"dateCreated": value})
        if label in ('Accession Number'.upper(), 'Rare Books Identifier'.upper(), 'Identifier'.upper()):
            mainOut.update({"identifier": value})
        if label in ('Artist'.upper(), 'Author'.upper(), 'Creator'.upper()):
            mainOut.update({"creator": value})
        if label in ('Date'.upper(), 'Dates'.upper()):
            mainOut.update({"temporalCoverage": value})
        if label in ('Language of Materials'.upper(), 'Language'.upper()):
            mainOut.update({"inLanguage": value})
        if label in ('Media'.upper(), 'Format'.upper()):
            mainOut.update({"matierial": value})
        i += 1

    return mainOut
