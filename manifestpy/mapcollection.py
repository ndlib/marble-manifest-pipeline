def mapManifestCollection(readfile, wtype):
    import os
    import json
    from mapmain import mapMainManifest

    path = os.path.dirname(os.path.abspath(__file__))
    wfile = os.path.join(path, 'outputmain.json')
    writemain = open(wfile, 'w+')

    mainOut = mapMainManifest(readfile, wtype)

    hasPart = []
    chldl = len(readfile['manifests'])
    j = 0
    while j < chldl:
        subfile = os.path.join(path, 'outputchild'+str(j)+'.json')
        thischild = readfile['manifests'][j]
        hasPart.append(subfile)
        writesub = open(subfile, 'w+')
        subOut = mapMainManifest(thischild, 'CreativeWork')
        subOut.update({"isPartOf": wfile})
        writesub.write(json.dumps(subOut))
        writesub.close()
        j += 1
    mainOut.update({"hasPart": hasPart})
    writemain.write(json.dumps(mainOut))
    writemain.close()
    return mainOut
