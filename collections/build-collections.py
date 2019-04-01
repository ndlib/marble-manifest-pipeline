import json
import os
from urllib.request import urlopen
import boto3


def write_file(dict, path):
    manifest_bucket = "mellon-manifest-prod-manifestbucket-1aed76g9eb0if"
    if not os.path.exists(os.path.dirname(path)):
        try:
            os.makedirs(os.path.dirname(path))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    with open(path, "w") as f:
        f.write(json.dumps(dict))

        s3 = boto3.resource('s3')
        print("writing:" + path)
        s3.Object(manifest_bucket, path).put(Body=json.dumps(dict), ACL='public-read', ContentType='text/json')


def get_manifest(id):
    print("Fetching: " + manifest_baseurl + id + '/index.json')
    return json.load(urlopen(manifest_baseurl + id + '/index.json'))


found_manifests = {}

manifest_baseurl = 'https://presentation-iiif.library.nd.edu/'

# these are the top level collection manifests created.
groups = [
    {
        "id": "website",
        "label": "All Manifests",
        "collections": [ "art-3", "le-rossignol", "art-1", "dante", "theophilus", "art-2", "journals" ],
        "description": "All the manifests!!",
        "viewingHint": "multi-part",
        "metadata": {},
        "license": "https://creativecommons.org/licenses/by-nc/4.0/"
    },
    {
        "id": "timeperiods",
        "label": "All the Timeperiod Manifests",
        "collections": [ "ancient-time-period", "medieval-time-period", "renaissance-time-period","18thcentury-time-period","19thcentury-time-period", "20thcentury-time-period", "21stcentury-time-period" ],
        "description": "All the manifests!!",
        "viewingHint": "multi-part",
        "metadata": {},
        "license": "https://creativecommons.org/licenses/by-nc/4.0/"
    },
    {
        "id": "places",
        "label": "All the Places Manifests",
        "collections": [ "ancient-time-period", "medieval-time-period", "renaissance-time-period" ],
        "description": "All the manifests!!",
        "viewingHint": "multi-part",
        "metadata": {},
        "license": "https://creativecommons.org/licenses/by-nc/4.0/"
    },
    {
        "id": "themes",
        "label": "All the Themes Manifests",
        "collections": [ "ancient-time-period", "medieval-time-period", "renaissance-time-period" ],
        "description": "All the manifests!!",
        "viewingHint": "multi-part",
        "metadata": {},
        "license": "https://creativecommons.org/licenses/by-nc/4.0/"
    }
]

# create on collection manifest for each collection
collections = {
    "epistemological-letters": {
        "id": "epistemological-letters",
        "manifest_ids": ['epistemological-letters-issue-1/manifest', 'epistemological-letters-issue-2/manifest', 'epistemological-letters-issue-3/manifest', 'epistemological-letters-issue-4/manifest', 'epistemological-letters-issue-5/manifest', 'epistemological-letters-issue-6/manifest', 'epistemological-letters-issue-7/manifest', 'epistemological-letters-issue-8/manifest'],
        "label": "Epistemological Letters",
        "description": "A written symposium on the topic of quantum physics edited by Abner Shimony and others published and distributed to a limited mailing list by Association Ferdinand Gonseth 1973-1984",
        "thumbnail": "https://image-iiif.library.nd.edu:8182/iiif/2/epistemological-letters-issue-2%2FMay19742ndIssue_Page_01.tif",
        "license": "Digitized and distributed with permission of Association Ferdinand Gonseth, the first to publish Epistemological Letters. CC BY-NC-ND",
        "metadata": [
        ]
    },
    "le-rossignol": {
        "id": "le-rossignol",
        "manifest_ids": ['le-rossignol-01/manifest', 'le-rossignol-02/manifest', 'le-rossignol-03/manifest', 'le-rossignol-04/manifest', 'le-rossignol-05/manifest', 'le-rossignol-06/manifest', 'le-rossignol-07/manifest', 'le-rossignol-08/manifest', 'le-rossignol-77/manifest', 'le-rossignol-81/manifest'],
        "label": "Le Rossignol Correspondence",
        "description": "The Le Rossignol correspondence consists of 45 letters written from Arthur Stanley Le Rossignol to his sister, Constance Ethel, from his position on the Western Front during World War I. There are additional letters written by Arthur (5), Ethel's colleague Pierre Pulinekse (4), Ethel herself to her Aunt Anna Le Rossignol (4), and others. The bulk of correspondence took place in 1918. The letters cover topics of cultural interest, such as British-German animosity and perceptions of the war. Most of Arthur's letters are vague and do not reveal his location or the details of military operations. His correspondence covers the period 1914-1919. All together, there are 52 letters, 42 envelopes, 14 post cards, a photograph album, a telegram, a copy of strategic battle plans, a pamphlet, and other miscellaneous items. ",
        "thumbnail": "https://image-iiif.library.nd.edu:8182/iiif/2/le-rossignol-81%2FMSE-MD_3821-081_00a.tif",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "1914-1919"},
            {"label": "Creator", "value": "Le Rossignol, Arthur Stanley, 1875-? (Person)"},
            {"label": "Language of Materials", "value": "English"},
            {"label": "Collection Finding Aid", "value": "<a href=\"https://archivesspace.library.nd.edu/repositories/3/resources/1392\">https://archivesspace.library.nd.edu/repositories/3/resources/1392</a>"}
        ]
    },
    "dante": {
        "id": "dante",
        "manifest_ids": ['ils-000909884/manifest', 'ils-000909885/manifest', 'ils-000909886/manifest', 'ils-000949761/manifest'],
        "label": "Dante",
        "description": "Lots of Dante",
        "thumbnail": "https://image-iiif.library.nd.edu:8182/iiif/2/ils-000909884%2FBOO_000909884-1-inf-02a.tif",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "1914-1919"},
            {"label": "Creator", "value": "Le Rossignol, Arthur Stanley, 1875-? (Person)"},
            {"label": "Language of Materials", "value": "English"},
            {"label": "Collection Finding Aid", "value": "<a href=\"https://archivesspace.library.nd.edu/repositories/3/resources/1392\">https://archivesspace.library.nd.edu/repositories/3/resources/1392</a>"}
        ]
    },
    "art-1": {
        "id": "art-1",
        "manifest_ids": ['1999.024/manifest', '1976.057/manifest'],
        "label": "Art Collection #1",
        "description": "Art",
        "thumbnail": "https://image-iiif.library.nd.edu:8182/iiif/2/1976.057%2F1976_057-v0001.tif",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "1914-1919"},
            {"label": "Creator", "value": "Art People"},
            {"label": "Language of Materials", "value": "English"},
        ]
    },
    "theophilus": {
        "id": "theophilus",
        "manifest_ids": ['theophilus-journal-v1/manifest', 'theophilus-journal-v2/manifest'],
        "label": "Theophilus Journal",
        "description": "Journal of Theophilus",
        "thumbnail": "https://image-iiif.library.nd.edu:8182/iiif/2/theophilus-journal-v1%2FMSN-EA_8011-01-B-000a.tif",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "1914-1919"},
            {"label": "Creator", "value": "Le Rossignol, Arthur Stanley, 1875-? (Person)"},
            {"label": "Language of Materials", "value": "English"},
            {"label": "Collection Finding Aid", "value": "<a href=\"https://archivesspace.library.nd.edu/repositories/3/resources/1392\">https://archivesspace.library.nd.edu/repositories/3/resources/1392</a>"}
        ]
    },
    "art-3": {
        "id": "art-3",
        "manifest_ids": ['1982.072.001/manifest', '1983.053.002/manifest', '1986.059.001/manifest'],
        "label": "Art Collection #3",
        "description": "ART ART ART ART ART ART ART ART ART ART ART",
        "thumbnail": "https://image-iiif.library.nd.edu:8182/iiif/2/1983.053.002%2F1983_053_002-v0001.tif",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "1914-1919"},
            {"label": "Creator", "value": "Art People"},
            {"label": "Language of Materials", "value": "English"},
        ]
    },
    "journals": {
        "id": "journals",
        "manifest_ids": ['abel-blanchard-correspondence/manifest', 'collection/theophilus', 'nathaniel-rogers-notebook/manifest'],
        "label": "Journals",
        "description": "Journals we have",
        "thumbnail": "https://image-iiif.library.nd.edu:8182/iiif/2/nathaniel-rogers-notebook%2FMSN-COL_9405-1-B-001v_002r.tif",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "1914-1919"},
            {"label": "Creator", "value": "Le Rossignol, Arthur Stanley, 1875-? (Person)"},
            {"label": "Language of Materials", "value": "English"},
            {"label": "Collection Finding Aid", "value": "<a href=\"https://archivesspace.library.nd.edu/repositories/3/resources/1392\">https://archivesspace.library.nd.edu/repositories/3/resources/1392</a>"}
        ]
    },
    "art-2": {
        "id": "art-2",
        "manifest_ids": ['1934.007.001/manifest'],
        "label": "Art Collection #2",
        "description": "More Art",
        "thumbnail": "https://image-iiif.library.nd.edu:8182/iiif/2/1934.007.001%2F1934_007_001-v0001.tif",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "1914-1919"},
            {"label": "Creator", "value": "Art People"},
            {"label": "Language of Materials", "value": "English"},
        ]
    },
    "ancient-time-period": {
        "id": "ancient-time-period",
        "manifest_ids": ["collection/art-3"],
        "label": "Ancient",
        "description": "All The \"Ancient\" items",
        "thumbnail": "https://image-iiif.library.nd.edu:8182/iiif/2/le-rossignol-81%2FMSE-MD_3821-081_00a.tif",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "A very very long time ago-A very long time ago"},
        ]
    },
    "medieval-time-period": {
        "id": "medieval-time-period",
        "manifest_ids": ['1934.007.001/manifest', 'collection/dante'],
        "label": "Medieval Time Period",
        "description": "All The \"Medieval\" items",
        "thumbnail": "https://image-iiif.library.nd.edu:8182/iiif/2/le-rossignol-81%2FMSE-MD_3821-081_00a.tif",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "1000-1400"},
        ]
    },
    "renaissance-time-period": {
        "id": "renaissance-time-period",
        "manifest_ids": ['collection/art-1'],
        "label": "Renaissance",
        "description": "All The \"Renaissance\" items",
        "thumbnail": "https://image-iiif.library.nd.edu:8182/iiif/2/le-rossignol-81%2FMSE-MD_3821-081_00a.tif",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "1400-1700"},
        ]
    },
    "18thcentury-time-period": {
        "id": "18thcentury-time-period",
        "manifest_ids": ['collection/journals', 'le-rossignol-01/manifest', 'le-rossignol-02/manifest', 'le-rossignol-03/manifest', 'le-rossignol-04/manifest'],
        "label": "18th Century",
        "description": "All The \"18th Century\" items",
        "thumbnail": "https://image-iiif.library.nd.edu:8182/iiif/2/le-rossignol-81%2FMSE-MD_3821-081_00a.tif",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
        ]
    },
    "19thcentury-time-period": {
        "id": "19thcentury-time-period",
        "manifest_ids": ['collection/theophilus'],
        "label": "19th Century",
        "description": "All The \"19th Century\" items",
        "thumbnail": "https://image-iiif.library.nd.edu:8182/iiif/2/le-rossignol-81%2FMSE-MD_3821-081_00a.tif",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
        ]
    },
    "20thcentury-time-period": {
        "id": "20thcentury-time-period",
        "manifest_ids": ['collection/le-rossignol'],
        "label": "20th Century",
        "description": "All The \"20th Century\" items",
        "thumbnail": "https://image-iiif.library.nd.edu:8182/iiif/2/le-rossignol-81%2FMSE-MD_3821-081_00a.tif",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
        ]
    },
    "21stcentury-time-period": {
        "id": "21thcentury-time-period",
        "manifest_ids": ['le-rossignol-01/manifest', 'le-rossignol-02/manifest', 'le-rossignol-03/manifest', 'le-rossignol-04/manifest'],
        "label": "21st Century",
        "description": "All The \"21st Century\" items",
        "thumbnail": "https://image-iiif.library.nd.edu:8182/iiif/2/le-rossignol-81%2FMSE-MD_3821-081_00a.tif",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
        ]
    },
}

# when i get the manifest for the item from the server copy these fields
copyFields = ['@id', '@type', 'label', 'metadata', 'thumbnail', 'description', "license"]

for collection_id in collections:
    print("working on: " + collection_id)
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
        if (found_manifests.get(id, False)):
            r = found_manifests[id]
        else:
            r = get_manifest(id)
            found_manifests[id] = r

        for key in copyFields:
            m[key] = r[key]

        collection["manifests"].append(m)

        write_file(collection, 'collection/' + data["id"] + '/index.json')
        found_manifests["collection/" + data["id"]] = collection


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
        collection["thumbnail"] = {"@id": data["thumbnail"] + "/full/250,/0/default.jpg", "service": {"@id": data["thumbnail"], "profile": "http://iiif.io/api/image/2/level2.json", "@context": "http://iiif.io/api/image/2/context.json"}}

        collection["metadata"] = data["metadata"]
        collection["license"] = data["license"]
        collection["manifests"] = []

        for id in data["manifest_ids"]:
            m = {}
            if (found_manifests.get(id, False)):
                r = found_manifests[id]
            else:
                r = get_manifest(id)
                found_manifests[id] = r

            for key in copyFields:
                m[key] = r[key]

            collection["manifests"].append(m)

        manifest["collections"].append(collection)

    write_file(manifest, 'collection/' + group["id"] + '/index.json')
