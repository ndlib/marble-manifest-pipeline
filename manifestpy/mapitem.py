def mapSingleItem(readfile, wtype):
    import json
    import os
    from mapmain import mapMainManifest

    path = os.path.dirname(os.path.abspath(__file__))
    wfile = os.path.join(path, 'outputmain.json')
    writemain = open(wfile, 'w+')
    mainOut = mapMainManifest(readfile, wtype)

    writemain.write(json.dumps(mainOut))
    writemain.close()
    return
