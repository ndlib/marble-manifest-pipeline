
import json, csv, os, glob, io
from urllib.request import urlopen
import boto3

def write_file(dict, path):
    manifest_bucket = "mellon-manifest-pipeline-manifestbucket-kel2eht9shpj"
    if not os.path.exists(os.path.dirname(path)):
        try:
            os.makedirs(os.path.dirname(path))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    with open(path, "w") as f:
        f.write(json.dumps(dict))

        s3 = boto3.resource('s3')
        s3.Object(manifest_bucket, path).put(Body=json.dumps(dict), ACL='public-read')


manifest_baseurl = 'https://d1v1nx8kcr1acm.cloudfront.net/'

# these are the top level collection manifests created.
groups = [
    {
        "id": "website",
        "label": "All Manifests",
        "collections": [ "le-rossignol" ],
        "description": "All the manifests!!",
        "viewingHint": "multi-part",
        "metadata": {},
        "license": "https://creativecommons.org/licenses/by-nc/4.0/"
    },
    {
        "id": "timeperiods",
        "label": "All the Timeperiod Manifests",
        "collections": [ "early-time-period", "middle-time-period", "late-time-period" ],
        "description": "All the manifests!!",
        "viewingHint": "multi-part",
        "metadata": {},
        "license": "https://creativecommons.org/licenses/by-nc/4.0/"
    }
]

# create on collection manifest for each collection
collections = {
    "le-rossignol": {
        "id": "le-rossignol",
        "manifest_ids": ['le-rossignol-01', 'le-rossignol-02', 'le-rossignol-03', 'le-rossignol-04', 'le-rossignol-05', 'le-rossignol-06', 'le-rossignol-07', 'le-rossignol-08', 'le-rossignol-77', 'le-rossignol-81'],
        "label": "Le Rossignol Correspondence",
        "description": "The Le Rossignol correspondence consists of 45 letters written from Arthur Stanley Le Rossignol to his sister, Constance Ethel, from his position on the Western Front during World War I. There are additional letters written by Arthur (5), Ethel's colleague Pierre Pulinekse (4), Ethel herself to her Aunt Anna Le Rossignol (4), and others. The bulk of correspondence took place in 1918. The letters cover topics of cultural interest, such as British-German animosity and perceptions of the war. Most of Arthur's letters are vague and do not reveal his location or the details of military operations. His correspondence covers the period 1914-1919. All together, there are 52 letters, 42 envelopes, 14 post cards, a photograph album, a telegram, a copy of strategic battle plans, a pamphlet, and other miscellaneous items. ",
        "thumbnail": "https://image-service-testlibnd-dev.library.nd.edu:8182/iiif/2/le-rossignol-81%2FMSE-MD_3821-081_00a.tif",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "1914-1919"},
            {"label": "Creator", "value": "Le Rossignol, Arthur Stanley, 1875-? (Person)"},
            {"label": "Language of Materials", "value": "English"},
            {"label": "Collection Finding Aid", "value": "<a href=\"https://archivesspace.library.nd.edu/repositories/3/resources/1392\">https://archivesspace.library.nd.edu/repositories/3/resources/1392</a>"}
        ]
    },
    "early-time-period": {
        "id": "early-time-period",
        "manifest_ids": ['le-rossignol-01', 'le-rossignol-02', 'le-rossignol-03', 'le-rossignol-04'],
        "label": "Early Time Period",
        "description": "All The \"Early\" items",
        "thumbnail": "https://image-service-testlibnd-dev.library.nd.edu:8182/iiif/2/le-rossignol-81%2FMSE-MD_3821-081_00a.tif",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "1914-1919"},
            {"label": "Creator", "value": "Le Rossignol, Arthur Stanley, 1875-? (Person)"},
            {"label": "Language of Materials", "value": "English"},
            {"label": "Collection Finding Aid", "value": "<a href=\"https://archivesspace.library.nd.edu/repositories/3/resources/1392\">https://archivesspace.library.nd.edu/repositories/3/resources/1392</a>"}
        ]
    },
    "middle-time-period": {
        "id": "middle-time-period",
        "manifest_ids": ['le-rossignol-05', 'le-rossignol-06', 'le-rossignol-07', 'le-rossignol-08'],
        "label": "Middle Time Period",
        "description": "All The \"Middle\" items",
        "thumbnail": "https://image-service-testlibnd-dev.library.nd.edu:8182/iiif/2/le-rossignol-81%2FMSE-MD_3821-081_00a.tif",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "1914-1919"},
            {"label": "Creator", "value": "Le Rossignol, Arthur Stanley, 1875-? (Person)"},
            {"label": "Language of Materials", "value": "English"},
            {"label": "Collection Finding Aid", "value": "<a href=\"https://archivesspace.library.nd.edu/repositories/3/resources/1392\">https://archivesspace.library.nd.edu/repositories/3/resources/1392</a>"}
        ]
    },
    "late-time-period": {
        "id": "late-time-period",
        "manifest_ids": ['le-rossignol-77', 'le-rossignol-81'],
        "label": "Late Time Period",
        "description": "All The \"Late\" items",
        "thumbnail": "https://image-service-testlibnd-dev.library.nd.edu:8182/iiif/2/le-rossignol-81%2FMSE-MD_3821-081_00a.tif",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "1914-1919"},
            {"label": "Creator", "value": "Le Rossignol, Arthur Stanley, 1875-? (Person)"},
            {"label": "Language of Materials", "value": "English"},
            {"label": "Collection Finding Aid", "value": "<a href=\"https://archivesspace.library.nd.edu/repositories/3/resources/1392\">https://archivesspace.library.nd.edu/repositories/3/resources/1392</a>"}
        ]
    }
}

# when i get the manifest for the item from the server copy these fields
copyFields = ['@id', '@type', 'label', 'metadata', 'thumbnail', 'description', "license"]


for group in groups:
    manifest = {}
    manifest["@context"] = "https://iiif.io/api/presentation/2/context.json"
    manifest["@id"] = manifest_baseurl + 'collection/' + group["id"]
    manifest["@type"] = "sc:Collection"
    manifest["label"] = group["label"]
    manifest["description"] = group["description"]
    manifest["license"] = group["license"]
    manifest["metadata"] = group["metadata"]
    manifest["viewingHint"] = group["viewingHint"]
    manifest["collections"] = []

    for collection_id in group["collections"]:
        data = collections[collection_id]
        collection = {}
        collection["@id"] = manifest_baseurl + 'collection/' + data["id"]
        collection["@type"] = "sc:Collection"
        collection["label"] = data["label"]
        collection["description"] = data["description"]
        collection["thumbnail"] = { "@id": data["thumbnail"] + "/full/250,/0/default.jpg", "service": {"@id": data["thumbnail"], "profile": "http://iiif.io/api/image/2/level2.json", "@context": "http://iiif.io/api/image/2/context.json" } }

        collection["metadata"]=data["metadata"]
        collection["license"]=data["license"]
        collection["manifests"] = []

        for id in data["manifest_ids"]:
            m = {}
            r = json.load(urlopen(manifest_baseurl + id + '/manifest/index.json'))
            for key in copyFields:
                m[key] = r[key]

            collection["manifests"].append(m)

        write_file(collection, 'collection/' + data["id"] + '/index.json')
        manifest["collections"].append(collection)

    write_file(manifest, 'collection/' + group["id"] +'/index.json')
