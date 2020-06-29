import json
from pathlib import Path


class MetadataMappings():

    def __init__(self, data):
        self.data = data
        self.provider = data.repository()
        self.lookup = self.loadMetadataRules()

    def loadMetadataRules(self):
        self.preferred = {}
        self.vracore = {}
        self.schema = {}
        self.element = {}
        self.standard_json = {}
        field_definitions_json = self.load_json_file("marble", self.provider.lower())
        for key, value in field_definitions_json.items():
            line = {
                "preferred_name": value['preferred name'],
                "schema.org": value['schema.org mapping'],
                "element": value['element'],
                "marble_title": value['marble display name'],
                "required": value['required']
            }
            if value.get('vra mapping', False):
                line["vracore"] = value['vra mapping']
                self.vracore[line['vracore'].lower()] = line

            self.preferred[line['preferred_name'].lower()] = line
            self.schema[line['schema.org'].lower()] = line
            self.element[line['element'].lower()] = line
            self.standard_json[key] = line

    def get_by_prefered(self, name, field):
        return self.preferred.get(name.lower()).get(field, False)

    def get_by_vracore(self, name, field):
        return self.vracore.get(name.lower()).get(field, False)

    def get_by_schema(self, name, field):
        return self.schema.get(name.lower()).get(field, False)

    def get_by_element(self, name, field):
        return self.element.get(name.lower()).get(field, False)

    def get_by_standard_json(self, name, field):
        return self.standard_json.get(name).get(field, False)

    def get_prefered_keys(self):
        return self.preferred.keys()

    def get_standard_json_keys(self):
        return self.standard_json.keys()

    def load_json_file(self, site_name: str, source_name: str) -> dict:
        current_path = str(Path(__file__).parent.absolute())
        with open(current_path + "/sites/" + site_name + "/" + source_name.lower() + ".json", 'r') as input_source:
            source = json.load(input_source)
        return source
