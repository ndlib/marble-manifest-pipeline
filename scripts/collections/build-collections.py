import json
import os
from urllib.request import urlopen
import boto3


def write_file(dict, path):
    manifest_bucket = "marble-manifest-prod-manifestbucket-lpnnaj4jaxl5"
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

        s3.Object(manifest_bucket, path).put(Body=json.dumps(dict), ContentType='text/json')


def get_manifest(id):
    print("Fetching: " + manifest_baseurl + id + '/index.json')
    return json.load(urlopen(manifest_baseurl + id + '/index.json'))


def fix_language(str):
    return {
        "en": [str]
    }


def fix_metadata(metadata):
    new = []
    for data in metadata:
        new.append({"label": fix_language(data.get('label')), "value":  fix_language(data.get('value'))})
    return new



found_manifests = {}
manifest_baseurl = 'https://presentation-iiif.library.nd.edu/'


# create on collection manifest for each collection
collections = {
    "epistemological-letters": {
        "id": "epistemological-letters",
        "manifest_ids": ['epistemological-letters-issue-1/manifest', 'epistemological-letters-issue-2/manifest', 'epistemological-letters-issue-3/manifest', 'epistemological-letters-issue-4/manifest', 'epistemological-letters-issue-5/manifest', 'epistemological-letters-issue-6/manifest', 'epistemological-letters-issue-7/manifest', 'epistemological-letters-issue-8/manifest'],
        "label": "Epistemological Letters",
        "description": "A written symposium on the topic of quantum physics edited by Abner Shimony and others published and distributed to a limited mailing list by Association Ferdinand Gonseth 1973-1984",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/epistemological-letters-issue-2%2FMay19742ndIssue_Page_01",
        "license": "Digitized and distributed with permission of Association Ferdinand Gonseth, the first to publish Epistemological Letters. CC BY-NC-ND",
        "metadata": [
        ]
    },
    "le-rossignol": {
        "id": "le-rossignol",
        "manifest_ids": ['le-rossignol-01/manifest', 'le-rossignol-02/manifest', 'le-rossignol-03/manifest', 'le-rossignol-04/manifest', 'le-rossignol-05/manifest', 'le-rossignol-06/manifest', 'le-rossignol-07/manifest', 'le-rossignol-08/manifest', 'le-rossignol-77/manifest', 'le-rossignol-81/manifest'],
        "label": "Le Rossignol Correspondence",
        "description": "The Le Rossignol correspondence consists of 45 letters written from Arthur Stanley Le Rossignol to his sister, Constance Ethel, from his position on the Western Front during World War I. There are additional letters written by Arthur (5), Ethel's colleague Pierre Pulinekse (4), Ethel herself to her Aunt Anna Le Rossignol (4), and others. The bulk of correspondence took place in 1918. The letters cover topics of cultural interest, such as British-German animosity and perceptions of the war. Most of Arthur's letters are vague and do not reveal his location or the details of military operations. His correspondence covers the period 1914-1919. All together, there are 52 letters, 42 envelopes, 14 post cards, a photograph album, a telegram, a copy of strategic battle plans, a pamphlet, and other miscellaneous items.",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/le-rossignol-01%2FMSE-MD_3821-001-env_a",
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
        "label": "Dante Alighieri's Divine Comedy",
        "description": "Versions of Dante Alighieri's Divine Comedy",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/ils-000909884%2FBOO_000909884-1-inf-02a",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "13th Century"},
            {"label": "Creator", "value": "Dante Aligieri 1265-1321"},
            {"label": "Language of Materials", "value": "13th Century Italian"},
            {"label": "Collection Finding Aid", "value": "<a href=\"https://archivesspace.library.nd.edu/repositories/3/resources/1392\">https://archivesspace.library.nd.edu/repositories/3/resources/1392</a>"}
        ]
    },
    "nd-life": {
        "id": "nd-life",
        "manifest_ids": ['CEDW-20-02-08/manifest', 'CEDW-30-16-01/manifest', 'CSOR-04-05-01/manifest', 'CTAO-01-28/manifest', 'GNDL-45-01/manifest', 'GNDL-45-02/manifest', 'GNDL-45-03/manifest', 'GNDL-45-04/manifest', 'GNDL-45-05/manifest', 'GNDL-45-06/manifest'],
        "label": "Notre Dame Life Collection",
        "description": "The Notre Dame Life Collection represents historical moments the university's past. Most of the images and artifacts are representative of 19th century moments closely following the founding of the university.",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/GNDL-45-04%2FGNDL-45-04",
        "license": "Materials from the Notre Dame Archives may be protected by copyright law under Title 17 U.S. Code.  Cannot be sold, given away, reproduced, or published in whole or in part (including on the internet) without the prior permission of the University of Notre Dame Archives.",
        "metadata": [
            {"label": "Dates", "value": "Mid to Late 19th Century"},
            {"label": "Creator", "value": "University of Notre Dame"},
        ]
    },
    "art-1": {
        "id": "art-1",
        "manifest_ids": ['1999.024/manifest', '1976.057/manifest'],
        "label": "Religious Paintings",
        "description": "Paintings from various time periods representing religious themes",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/1976.057%2F1976_057-v0001",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "17th through 19th Centuries"},
            {"label": "Theme", "value": "Religious"},
            {"label": "Medium", "value": "Oil on canvas"},
        ]
    },
    "theophilus": {
        "id": "theophilus",
        "manifest_ids": ['theophilus-journal-v1/manifest', 'theophilus-journal-v2/manifest'],
        "label": "Theophilus Parsons Journal",
        "description": "Journal of Theophilus Parsons",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/theophilus-journal-v1%2FMSN-EA_8011-01-B-000a",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "Late 18th through early 19th Centuries"},
            {"label": "Creator", "value": "Theophilus Parsons 1750-1813"},
            {"label": "Language of Materials", "value": "English"},
            {"label": "Format", "value": "Personal Journal"},
            {"label": "Collection Finding Aid", "value": "<a href=\"https://archivesspace.library.nd.edu/repositories/3/resources/1392\">https://archivesspace.library.nd.edu/repositories/3/resources/1392</a>"}
        ]
    },
    "art-3": {
        "id": "art-3",
        "manifest_ids": ['1982.072.001/manifest', '1983.053.002/manifest', '1986.059.001/manifest'],
        "label": "Historical Artifacts",
        "description": "Historical artifacts from various time periods",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/1983.053.002%2F1983_053_002-v0001",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "3rd through 19th Centuries"},
            {"label": "Materials", "value": "Woven cloth, metal and stone"},
        ]
    },
    "journals": {
        "id": "journals",
        "manifest_ids": ['nduspec_eadks65h991878/manifest', 'collection/theophilus', 'nduspec_ead7s75db80w4r/manifest'],
        "label": "Historical Journals",
        "description": "A collection of historical journals, letters and other personal correspondence",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/nduspec_ead7s75db80w4r%2FMSN-COL_9405-1-B-001v_002r",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "19th Century"},
            {"label": "Creator", "value": "Various Authors"},
            {"label": "Language of Materials", "value": "English"}
        ]
    },
    "art-2": {
        "id": "art-2",
        "manifest_ids": ['1934.007.001/manifest', '1982.072.001/manifest'],
        "label": "Religious Artifacts",
        "description": "Liturgical and religious artifacts from the 19th century",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/1934.007.001%2F1934_007_001-v0001",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "19th Century"},
            {"label": "Creator", "value": "Anonymous"},
            {"label": "Materials", "value": "Gilt polychrome wood, cloth and leather"},
        ]
    },
    "north-america": {
        "id": "north-america",
        "manifest_ids": ['1986.059.001/manifest', '1982.072.001/manifest', '1976.057/manifest', 'collection/theophilus', 'collection/journals', 'nduspec_ead7s75db80w4r/manifest', '1934.007.001/manifest', 'collection/nd-life' ],
        "label": "North America",
        "description": "Items that depict or have their origin in North America",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/1976.057%2F1976_057-v0001",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "19th and 20th Centuries"},
            {"label": "Materials", "value": "This collection includes items in a variety of media from paintings to printed pamphlets"},
        ]
    },
    "south-america": {
        "id": "south-america",
        "manifest_ids": ['1983.053.002/manifest'],
        "label": "South America",
        "description": "Items that depict or have their origin in South America",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/1983.053.002%2F1983_053_002-v0001",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "Ancient"},
            {"label": "Creator", "value": "Anonymous"},
            {"label": "Materials", "value": "Greenstone and other materials"},
        ]
    },
    "europe": {
        "id": "europe",
        "manifest_ids": ['1999.024/manifest', 'collection/dante', 'collection/epistemological-letters', 'collection/le-rossignol'],
        "label": "Europe",
        "description": "Items that depict or have their origin in Europe",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/ils-000909884%2FBOO_000909884-1-inf-02a",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "19th Century"},
            {"label": "Creator", "value": "Anonymous"},
            {"label": "Materials", "value": "Gilt polychrome wood, cloth and leather"},
        ]
    },
    "religious": {
        "id": "religious",
        "manifest_ids": ['1986.059.001/manifest', '1982.072.001/manifest', '1934.007.001/manifest', 'CSOR-04-05-01/manifest', 'GNDL-45-01/manifest', 'nduspec_ead7s75db80w4r/manifest', 'CTAO-01-28/manifest'],
        "label": "Religious Artifacts",
        "description": "From liturgical dress to altar ornamentation to sermon manuscripts; explore the rich collections at the University of Notre Dame used in religious ceremonies and inspired by world religions.",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/CSOR-04-05-01%2FCSOR-04-05-01",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "19th Century"},
            {"label": "Creator", "value": "Anonymous"},
            {"label": "Materials", "value": "Gilt polychrome wood, cloth and leather"},
        ]
    },
    "historical": {
        "id": "historical",
        "manifest_ids": ['1983.053.002/manifest', 'collection/le-rossignol', '1976.057/manifest', 'nduspec_eadks65h991878/manifest', 'nduspec_ead7s75db80w4r/manifest', 'collection/theophilus'],
        "label": "Historical Artifacts",
        "description": "Artifacts that have particular historical significance",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/GNDL-45-05%2FGNDL-45-05",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "19th Century"},
            {"label": "Creator", "value": "Anonymous"},
            {"label": "Materials", "value": "Gilt polychrome wood, cloth and leather"},
        ]
    },
    "personal": {
        "id": "personal",
        "manifest_ids": ['collection/journals', 'collection/le-rossignol', 'collection/epistemological-letters', 'CSOR-04-05-01/manifest', 'CEDW-30-16-01/manifest'],
        "label": "Journals and Notebooks",
        "description": "Personal correspondence, journals, notes, and diaries from various time periods",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/nduspec_eadks65h991878%2FMSN-EA_5031-01.a",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "19th Century"},
            {"label": "Creator", "value": "Anonymous"},
            {"label": "Materials", "value": "Gilt polychrome wood, cloth and leather"},
        ]
    },
    "science": {
        "id": "science",
        "manifest_ids": ['GNDL-45-05/manifest', 'collection/epistemological-letters'],
        "label": "Scientific Artifacts",
        "description": "Artifacts that are related to the history of science or are of scientific interest",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/epistemological-letters-issue-2%2FMay19742ndIssue_Page_01",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "19th Century"},
            {"label": "Creator", "value": "Anonymous"},
            {"label": "Materials", "value": "Gilt polychrome wood, cloth and leather"},
        ]
    },
    "notre-dame": {
        "id": "notre-dame",
        "manifest_ids": ['GNDL-45-02/manifest', 'GNDL-45-04/manifest', 'GNDL-45-05/manifest', 'CEDW-20-02-08/manifest', '1976.057/manifest'],
        "label": "Notre Dame",
        "description": "The University of Notre Dame was founded in 1842 by a small group of priests from the Congregation of the Holy Cross. Well-known for its sports programs and Catholic heritage, the university today is a site for cutting-edge research and rigorous liberal arts scholarship. The University of Notre Dame holds most of the photographs, documents, and related materials chronicling the university’s long history. The Snite Museum of Art also stewards many important portraits, prints, and paintings relating to significant figures in the university’s history.",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/GNDL-45-04%2FGNDL-45-04",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "19th Century"},
            {"label": "Creator", "value": "Anonymous"},
            {"label": "Materials", "value": "Gilt polychrome wood, cloth and leather"},
        ]
    },
    "ancient-time-period": {
        "id": "ancient-time-period",
        "manifest_ids": ['1983.053.002/manifest'],
        "label": "0-5th Century",
        "description": "Artifacts from pre-historical or early first millenial time periods",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/1983.053.002%2F1983_053_002-v0001",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "Pre-historical or early first millenia"},
        ]
    },
    "medieval-time-period": {
        "id": "medieval-time-period",
        "manifest_ids": ['1934.007.001/manifest', 'collection/dante'],
        "label": "5th Century-14th Century",
        "description": "Artifacts that originate in or depict aspects of the time period ranging from the 11th through 15th centuries",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/1934.007.001%2F1934_007_001-v0001",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "1000-1400"},
        ]
    },
    "renaissance-time-period": {
        "id": "renaissance-time-period",
        "manifest_ids": ['collection/dante', '1999.024/manifest', 'nduspec_ead7s75db80w4r/manifest'],
        "label": "14th Century-18th Century",
        "description": "Artifacts dating roughly to the 14th through 16th centuries",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/ils-000949761%2FBOO_000949761_c2-000ba",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
            {"label": "Dates", "value": "1400-1600"},
        ]
    },
    "18thcentury-time-period": {
        "id": "18thcentury-time-period",
        "manifest_ids": ['nduspec_eadks65h991878/manifest'],
        "label": "18th Century",
        "description": "Artifacts that have their origin in or reference the 18th century",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/nduspec_eadks65h991878%2FMSN-EA_5031-01.a",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
        ]
    },
    "19thcentury-time-period": {
        "id": "19thcentury-time-period",
        "manifest_ids": ['collection/theophilus', '1982.072.001/manifest', 'collection/nd-life', 'nduspec_eadks65h991878/manifest', '1976.057/manifest'],
        "label": "19th Century",
        "description": "Artifacts that have their origin in or reference the 19th century",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/theophilus-journal-v1%2FMSN-EA_8011-01-B-000a",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
        ]
    },
    "20thcentury-time-period": {
        "id": "20thcentury-time-period",
        "manifest_ids": ['collection/le-rossignol', '1986.059.001/manifest', 'collection/epistemological-letters'],
        "label": "20th Century",
        "description": "Artifacts that have their origin in or reference the 20th century",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/epistemological-letters-issue-2%2FMay19742ndIssue_Page_01",
        "license": "https://creativecommons.org/licenses/by-nc/4.0/",
        "metadata": [
        ]
    },
}


# when i get the manifest for the item from the server copy these fields
copyFields = ['id', 'type', 'label', 'metadata', 'thumbnail', 'summary', "rights"]

for collection_id in collections:
    print("working on: " + collection_id)
    data = collections[collection_id]
    collection = {}
    collection["id"] = manifest_baseurl + 'collection/' + data["id"]
    collection["type"] = "Collection"
    collection["label"] = fix_language(data["label"])
    collection["summary"] = fix_language(data["description"])
    collection["thumbnail"] = [{ "id": data["thumbnail"] + "/full/250,/0/default.jpg", "service": [{"id": data["thumbnail"], "profile": "http://iiif.io/api/image/2/level2.json", "type": "ImageService2" }] }]

    collection["metadata"]=fix_metadata(data["metadata"])
    collection["rights"]=data["license"]
    collection["items"] = []

    for id in data["manifest_ids"]:
        m = {}
        if (found_manifests.get(id, False)):
            r = found_manifests[id]
        else:
            r = get_manifest(id)
            found_manifests[id] = r

        for key in copyFields:
            m[key] = r.get(key, '')

        collection["items"].append(m)

    write_file(collection, 'collection/' + data["id"] + '/index.json')
    found_manifests["collection/" + data["id"]] = collection
