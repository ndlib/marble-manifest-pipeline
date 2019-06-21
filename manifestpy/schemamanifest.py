def main():
    import os
    import json
    from manifestmap import mapManifestOfItems
    from mapitem import mapSingleItem
    from mapcollection import mapManifestCollection
    path = os.path.dirname(os.path.abspath(__file__))
    rfile = os.path.join(path, 'manifest.json')
    with open(rfile, 'r') as f:
        readfile = json.load(f)
        type = readfile['@type']
    if type == 'sc:Collection':
        mapManifestCollection(readfile, 'CreativeWorkSeries')
    elif type == 'sc:Manifest':
        if readfile['sequences']:
            mapManifestOfItems(readfile, 'CreativeWork')
        else:
            mapSingleItem(readfile, 'CreativeWork')
    else:
        print 'Unknown Manifest'


if __name__ == '__main__':
    main()
