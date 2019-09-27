import json
import csv
from io import StringIO


class CsvToSchema():
    def __init__(self, config, main_csv, items_csv, image_data):
        self.dict = self.csv_to_dict(main_csv, items_csv)
        self.id = self.dict['unique_identifier']
        self.base_url = config['manifest-server-base-url'] + "/" + self.id
        self.default_image = {}
        self.has_part_items = self.has_part_items()
        self.main_item = self.main_item
        self.errors = []

    def get_json(self):
        graph = [self.main_item] + self.has_part_items
        return {
            "@context": "http://schema.org",
            "@graph": graph
        }

    def main_item(self):
        fieldmap = self.mappings()

        main = {"@id": self.base_url, "@type": "CreativeWork"}
        for key, val in self.dict.items():
            if key.lower() in fieldmap:
                if self.dict[key].strip():
                    main.update({fieldmap[key.lower()]: self.dict[key]})
            else:
                self.errors.append(key + ' has no value assigned \n')

        main['thumbnail'] = self.default_image
        main["hasPart"] = []
        for hasPartItem in self.has_part_items:
            main["hasPart"].append(hasPartItem["@id"])

        return main

    def csv_to_dict(self, main_csv, items_csv):
        dict = {"items": []}
        f = StringIO(items_csv)
        reader = csv.DictReader(f, delimiter=',')
        for this_row in reader:
            if reader.line_num != 1:
                dict["items"].append(this_row)

        f = StringIO(main_csv)
        reader = csv.DictReader(f, delimiter=',')
        for this_row in reader:
            if reader.line_num == 2:
                dict.update(this_row)
                del dict["Metadata_label"]
                del dict["Metadata_value"]
            elif reader.line_num > 2:
                dict[this_row["Metadata_label"].lower()] = this_row['Metadata_value']

        return dict

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
          "label": "name",
          "alternate title": "alternateName",
          "summary": "description",
          "artist": "creator",
          "author": "creator",
          "creator": "creator",
          "date": "temporalCoverage",
          "dates": "temporalCoverage",
          "creation date": "dateCreated",
          "classification": "description",
          "media": "description",
          "format": "material",
          "media": "material",
          "accession number": "identifier",
          "rare books identifier": "identifier",
          "identifier": "identifier",
          "repository": "provider",
          "attribution": "provider",
          "keywords": "keywords",
          "rights": "copyrightHolder",
          "rights": "license",
          "license": "license",
          "id": "url",
          "unique_identifier": "identifier",
          "contributor": "publisher",
          "access rights": "conditionOfAccess",
          "language of material": "inLanguage",
          "dimensions": "materialExtent",
          "attribution": "sponsor",
          "description": "disambiguatingDescription",
          "thumbnail": "thumbnailURL"
        }
