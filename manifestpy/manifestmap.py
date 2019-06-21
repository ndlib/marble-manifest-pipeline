def mapManifestOfItems(readfile, wtype):
    import os
    from mapmain import mapMainManifest

    path = os.path.dirname(os.path.abspath(__file__))
    wfile = os.path.join(path, 'outputmain.json')
    w = open(wfile, 'w+')

    mainOut = mapMainManifest(readfile, wtype)

    mainOut += '"hasPart":['
    chldl = len(readfile['sequences'][0]['canvases'])
    j = 0
    while j < chldl:
        subfile = os.path.join(path, 'outputchild'+str(j)+'.json')
        thischild = readfile['sequences'][0]['canvases'][j]
        mainOut += '"' + subfile + '",'
        s = open(subfile, 'w+')
        subOut = '{"@context":"http://schema.org",'
        subOut += '"@type":"CreativeWork",'
        subOut += '"isPartOf":"' + wfile + '",'
        subOut += '"name":"' + thischild['label'] + '",'
        subOut += '"url":"' + thischild['@id'] + '",'
        subOut += '"thumbnailURL":"' + thischild['thumbnail']['@id'] + '"}'
        s.write(subOut)
        s.close()
        j += 1
    mainOut += ']}'
    w.write(mainOut)
    w.close()
    return
