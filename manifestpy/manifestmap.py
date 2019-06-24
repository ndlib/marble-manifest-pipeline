def mapManifestOfItems(readfile, wtype):
    import os
    import json
    from mapmain import mapMainManifest

    path = os.path.dirname(os.path.abspath(__file__))
    wfile = os.path.join(path, 'outputmain.json')
    writemain = open(wfile, 'w+')

    mainOut = mapMainManifest(readfile, wtype)

    hasPart = []
    chldl = len(readfile['sequences'][0]['canvases'])
    j = 0
    while j < chldl:
        subfile = os.path.join(path, 'outputchild'+str(j)+'.json')
        thischild = readfile['sequences'][0]['canvases'][j]
        hasPart.append(subfile)
        writesub = open(subfile, 'w+')
        subOut = {
            "@context": "http://schema.org",
            "@type": "CreativeWork",
            "isPartOf": wfile,
            "name": thischild['label'],
            "url": thischild['@id'],
            "thumbnailURL": thischild['thumbnail']['@id']
        }
        writesub.write(json.dump(subOut))
        writesub.close()
        j += 1
    mainOut.update({"hasPart": hasPart})
    writemain.write(json.dump(mainOut))
    writemain.close()
    return
