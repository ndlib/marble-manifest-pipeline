from pathlib import Path
import os
from MetadataMappings import MetadataMappings


class ToSchema():
    def __init__(self, id, config, data, image_data=False):
        self.errors = []
        self.config = config
        self.image_data = image_data
        self.dict = data
        self.id = id
        self.base_url = config['manifest-server-base-url'] + "/" + self.id
        self.default_image = {}
        self.has_part_items = self.has_part_items()
        self.main_item = self.main_item()

    def get_json(self):
        graph = [self.main_item] + self.has_part_items
        return {
            "@context": "http://schema.org",
            "@graph": graph
        }

    def main_item(self):
        fieldmap = self.mappings()

        main = {"@id": self.base_url, "@type": "CreativeWork"}
        for key in fieldmap.get_athena_keys():
            if self.dict.get(key):
                main.update({fieldmap.get_by_athena(key, 'schema.org'): self.dict.get(key)})
            else:
                self.errors.append(key + ' has no value assigned \n')

        main["provider"] = self.dict.repository()
        main['thumbnail'] = self.default_image
        main["hasPart"] = []
        for hasPartItem in self.has_part_items:
            main["hasPart"].append(hasPartItem["@id"])

        return main

    def has_part_items(self):
        ret = []
        return []
        for index, item in enumerate(self.dict['items']):
            id = self.base_url + "%2F" + item["Filenames"]
            file = Path(item["Filenames"]).stem

            schemaImage = {
                "@id": id,
                "@type": "ImageObject",
                "name": item["Label"],
                "caption": item["Description"],
                "contentUrl": self.config['image-server-base-url'] + "/" + self.id + "%2F" + self.remomve_file_extension(item["Filenames"]),
                "position": str(index + 1),
                "isPartOf": self.base_url,
                "height": str(self.image_data[file]['height']),
                "width": str(self.image_data[file]['width']),
                "identifier": item["Filenames"],
            }
            ret.append(schemaImage)
            if (item.get('DefaultImage', False) == 'yes'):
                self.default_image = id

        if (not self.default_image):
            self.default_image = ret[0]['@id']

        return ret

    def mappings(self):
        return MetadataMappings(self.dict)

    def remomve_file_extension(self, file):
        return os.path.splitext(file)[0]
