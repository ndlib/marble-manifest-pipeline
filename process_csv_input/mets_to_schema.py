import json
import xml.etree.ElementTree as ET
import csv
from io import StringIO


class MetsToSchema():
    def __init__(self, config, descriptive_metadata, structural_metadata, image_data):
        self.dict = self.descriptive_to_dict(descriptive_metadata)
        self.id = self.dict.findall('.//dcterms:identifier').text
        self.base_url = config['manifest-server-base-url'] + "/" + self.id
        self.default_image = {}
        #self.has_part_items = self.has_part_items()
        self.main_item = self.main_item
        self.errors = []
        self.json = self.map()

    def get_json(self):
        graph = [self.main_item] + self.has_part_items
        return {
            "@context": "http://schema.org",
            "@graph": graph
        }

    def main_item(self):
        fieldmap = self.mappings()

        main = {"@id": self.base_url, "@type": "CreativeWork"}
        for key, val in fieldmap:
            if this.dict.findall(val):
                main[key] = val.findall(val)

        #main['thumbnail'] = self.default_image
        #main["hasPart"] = []
        #for hasPartItem in self.has_part_items:
        #    main["hasPart"].append(hasPartItem["@id"])

        return main

    def descriptive_to_dict(self, descriptive_metadata):
        return ET.fromstring(descriptive_metadata)

        return root.findall('mets:file')

        return xmltodict.parse(descriptive_metadata)

    def has_part_items(self):
        ret = []
        for index, item in enumerate(self.dict['items']):
            id = self.base_url + "/" + item["Filenames"]

            schemaImage = {
                "@id": id,
                "@type": "ImageObject",
                "name": item["Label"],
                "caption": item["Description"],
                "contentUrl": item["Filenames"],
                "position": index + 1,
                "isPartOf": self.base_url,
                "identifier": self.id + "%2F" + item["Filenames"],
                "representativeOfPage": True
            }
            ret.append(schemaImage)
            if (item['DefaultImage'] == 'yes'):
                self.default_image = id

        if (not self.default_image):
            self._default_image = ret[0]['@id']

        return ret

    def mappings(self):
        return {
          "name": "dcterms:title",
          "creator": "dcterms:creator",
          "temporalCoverage": "dcterms:created",
          "description": "dcterms:format",
          "material": "vracore:display",
          "identifier": "dcterms:identifier",
          "provider": "dcterms:publisher",
          "keywords": "dcterms:subject",
          "copyrightHolder": "dcterms:rights",
          "license": "dcterms:license",
          "conditionOfAccess": "dcterms:accessRights",
          "materialExtent": "dcterms:extent",
          "sponsor": "dcterms:provenance",
          "disambiguatingDescription": "dcterms:description",
          "dateModified": "vracore:latestDate"
        }
