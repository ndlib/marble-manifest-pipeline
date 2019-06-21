def mapSingleItem(readfile, wtype):
    import re
    import os
    import json
    from mapmain import mapMainManifest

    path = os.path.dirname(os.path.abspath(__file__))
    wfile = os.path.join(path, 'outputmain.json')
    w = open(wfile, 'w+')
    mainOut = mapMainManifest(readfile, wtype) + '}'

    w.write(mainOut)
    w.close()
    return
