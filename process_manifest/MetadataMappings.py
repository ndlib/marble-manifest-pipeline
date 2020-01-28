import csv
from io import StringIO
from pathlib import Path


class MetadataMappings():

    def __init__(self, data):
        self.data = data
        self.provider = data.repository()
        self.lookup = self.loadFile()

    def loadFile(self):
        self.preferred = {}
        self.vracore = {}
        self.schema = {}
        self.element = {}
        self.athena = {}
        f = StringIO(self.file())
        reader = csv.DictReader(f, delimiter=',')
        for this_row in reader:
            if reader.line_num != 1:
                athena_key = "".join(this_row['Preferred Name'].split()).lower()
                line = {
                    "preferred_name": this_row['Preferred Name'],
                    "schema.org": this_row['Schema.org mapping'],
                    "element": this_row['Element'],
                    "marble_title": this_row['MARBLE Display Name'],
                    "required": this_row['Required'],
                    "athena_name": athena_key
                }
                if this_row.get('VRA Mapping', False):
                    line["vracore"] = this_row['VRA Mapping']
                    self.vracore[line['vracore'].lower()] = line

                self.preferred[line['preferred_name'].lower()] = line
                self.schema[line['schema.org'].lower()] = line
                self.element[line['element'].lower()] = line
                self.athena[athena_key] = line

    def get_by_prefered(self, name, field):
        return self.preferred.get(name.lower()).get(field, False)

    def get_by_vracore(self, name, field):
        return self.vracore.get(name.lower()).get(field, False)

    def get_by_schema(self, name, field):
        return self.schema.get(name.lower()).get(field, False)

    def get_by_element(self, name, field):
        return self.element.get(name.lower()).get(field, False)

    def get_by_athena(self, name, field):
        return self.athena.get(name.lower()).get(field, False)

    def get_prefered_keys(self):
        return self.preferred.keys()

    def get_athena_keys(self):
        return self.athena.keys()

    def file(self):
        current_path = str(Path(__file__).parent.absolute())
        with open(current_path + "/" + self.provider.lower() + ".csv", 'r') as input_source:
            source = input_source.read()
        input_source.close()
        return source
