def main():
    import os
    import xmltodict
    import json
    from mapmetsmain import mapMetsManifest

    path = os.path.dirname(os.path.abspath(__file__))
    rfile = os.path.join(path, '1957.058.xml')
    with open(rfile, 'r') as f:
        readfile = xmltodict.parse(f.read())

    mainOut = mapMetsManifest(readfile, 'CreativeWork')
    wfile = os.path.join(path, 'outputmainmets.json')
    writemain = open(wfile, 'w+')
    writemain.write(json.dumps(mainOut))
    return


if __name__ == '__main__':
    main()
