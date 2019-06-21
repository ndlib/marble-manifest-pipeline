def mapManifestCollection(readfile, wtype):
    import os
    from mapmain import mapMainManifest

    path = os.path.dirname(os.path.abspath(__file__))
    wfile = os.path.join(path, 'outputmain.json')
    w = open(wfile, 'w+')

    mainOut = mapMainManifest(readfile, wtype)

    mainOut += '"hasPart":['
    chldl = len(readfile['manifests'])
    j = 0
    while j < chldl:
        subfile = os.path.join(path, 'outputchild'+str(j)+'.json')
        thischild = readfile['manifests'][j]
        mainOut += '"' + subfile + '",'
        s = open(subfile, 'w+')
        subOut = mapMainManifest(thischild, 'CreativeWork')
        subOut += '"isPartOf":"' + wfile + '"}'
        s.write(subOut)
        s.close()
        j += 1
    mainOut += ']}'
    w.write(mainOut)
    w.close()
    return
