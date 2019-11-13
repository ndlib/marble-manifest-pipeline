import json
import os
from urllib.request import urlopen
import urllib.parse
import boto3


def write_file(dict, path):
    manifest_bucket = "marble-manifest-prod-manifestbucket-lpnnaj4jaxl5"
    #manifest_bucket = "marble-manifest-prod-manifestbucket-jzzipymzclzw"

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
    print("Fetching: " + manifest_baseurl + id)
    id = urllib.parse.quote(id)
    return json.load(urlopen(manifest_baseurl + id))


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
    "MSN-EA_8011": {
        "id": "MSN-EA_8011",
        "manifest_ids": ['MSN-EA_8011-1-B/manifest', 'MSN-EA_8011-2-B/manifest'],
        "label": "Theophilus Parsons Journal",
        "description": "Parsons' two-volume journal dates from his early professional years, with entries running from January 1819 (when he opened his Boston law practice) to March 1823. The volumes are bound in polished calf, with Parsons' name stamped in gilt on the covers. Entries are irregular but often lengthy, running to a total of perhaps 60,000 words. The journal discusses aspects of Parsons' personal, professional, and intellectual/spiritual life with what appears to be a high degree of candor. Topics include the courtship of his future wife, Catherine; travel through New York and New England; detailed accounts of his extensive and eclectic reading; literary efforts; and a budding legal career, including work on an early fugitive slave case. There is discussion of what proved to be influential cultural events, like the Swedenborgian Sampson Reed's \"Oration on Genius,\" heard by Parsons (and Emerson) at Harvard in 1821. There is also a great deal on Boston and Cambridge intellectual life generally. Parsons travelled in elevated social circles, and mention is made of many of New England's first families. As he makes clear in a preface to volume 1, Parsons regarded his \"journalising\" as a vehicle for self-improvement, and strengths and (more commonly) inadequacies of character are frequently meditated upon. Parsons is no Calvinist, but the introspective nature of his entries places his journal in the tradition of New England forebears like Samuel Sewall. ",
        "thumbnail": "https://image-iiif.library.nd.edu/iiif/2/MSN-EA_8011-1-B%2FMSN-EA_8011-01-B-000a",
        "license": "http://rightsstatements.org/vocab/NoC-US/1.0/",
        "metadata": [
            {"label": "Identifier", "value": "MSN/EA 8011"},
            {"label": "Dates", "value": "1819-1823"},
            {"label": "Creator", "value": "Parsons, Theophilus, 1797-1882"},
            {"label": "Language of Materials", "value": "English"},
            {"label": "Biographical / Historical", "value": "Theophilus Parsons (1797-1882), a son of the noted Massachusetts jurist of the same name, was a Boston lawyer and literary figure best remembered for the legal texts he authored during his long tenure as Dane Professor of Law at Harvard. He was also a man of literary, philosophical, and religious interests, editing several journals and writing on a variety of topics (including Swedenborgianism, a preoccupation he shared with the Transcendentalists). Born in Newburyport and raised in Boston, Parsons graduated from Harvard in 1815 and went on to study law in the office of William Prescott; in 1819 he opened a law office in Boston. From 1822-27 Parsons lived in Taunton, Massachusetts, serving briefly as a representative to the state legislature. In 1823—the year of his marriage to Catherine Amory Chandler—he converted to the Swedenborgian faith. Though called by one of his contemporaries \"really more of a littérateur than a lawyer\" he effectively pursued both careers, building his legal practice while writing on literary and philosophical topics and editing journals like The United States Literary Gazette (which he founded in 1825) and The New-England Galaxy. In 1848 he accepted an appointment as Dane Professor of Law at Harvard, a position he held until 1870. In the 1850s Parsons published a succession of legal texts, most notably The Law of Contracts, (1853-55) which by 1904 had run through nine editions. Oliver Wendell Holmes, Jr. noted that Parsons was \". . . almost if not quite, a man of genius and gifted with a power of impressive statement which I do not know that I have ever seen equaled.\" After leaving Harvard Parsons remained in Cambridge, pursuing literary interests. "},
            {"label": "Extent", "value": ".25 Cubic Feet"},
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
copyFields = ['id', 'type', 'label', 'thumbnail', "rights"]

for collection_id in collections:
    print("working on: " + collection_id)
    data = collections[collection_id]
    collection = {}
    collection["@context"] = "http://iiif.io/api/presentation/3/context.json"
    collection["id"] = manifest_baseurl + 'collection/' + data["id"]
    collection["type"] = "Collection"
    collection["label"] = fix_language(data["label"])
    collection["summary"] = fix_language(data["description"])
    collection["thumbnail"] = [{"id": data["thumbnail"] + "/full/250,/0/default.jpg", "type": "Image", "service": [{"id": data["thumbnail"], "type": "ImageService2", "profile": "http://iiif.io/api/image/2/level2.json"}]}]
    collection["behavior"] = [ "multi-part" ]
    collection["metadata"] = fix_metadata(data["metadata"])
    collection["rights"] = data["license"]
    collection["items"] = []

    for id in data["manifest_ids"]:
        m = {}
        if (found_manifests.get(id, False)):
            r = found_manifests[id]
        else:
            r = get_manifest(id)
            found_manifests[id] = r

        for key in copyFields:
            if (r.get(key, False)):
                m[key] = r.get(key, '')

        collection["items"].append(m)

    write_file(collection, 'collection/' + data["id"] + '/index.json')
    found_manifests["collection/" + data["id"]] = collection
