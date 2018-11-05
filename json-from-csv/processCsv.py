import json, csv, os


#start with an empty result json


class processCsv():
    # class constructor
    def __init__(self):
        self.result_json = {}
        self.config = {}
        self._get_config_param()
        self._set_json_skeleton()
        self.main_csv = "../example/example-main.csv"
        self.sequence_csv = "../example/example-sequence.csv"

    def _get_config_param(self):
        self.config['server_url']='https://image-server.library.nd.edu:8182'
        self.config['path_prefix']='/iiif/2'

    # set up framework of an empty results_json 
    def _set_json_skeleton(self):
        self.result_json['errors']=[]
        self.result_json['creator']='creator@email.com'
        self.result_json['metadata']=[]
        self.result_json['sequences']=[]
        self.result_json['sequences'].append({})
        self.result_json['sequences'][0]['pages']=[]

    # process first data row of main CSV
    def _get_attr_from_main_firstline( self, first_line ):
        self.result_json['label'] = first_line['Label']
        self.result_json['description'] = first_line['Description']
        self.result_json['attribution'] = first_line['Attribution']
        self.result_json['rights'] = first_line['Rights']
        self.result_json['unique-identifier'] = first_line['unique_identifier']
        self.result_json['iiif-server'] = self.config['server_url'] + self.config['path_prefix']
        self.result_json['sequences'][0]['viewingHint'] = first_line['Sequence_viewing_experience']
        self.result_json['sequences'][0]['label'] = first_line['Sequence_label']

    # process a metadata lable/value only row from the main CSV (any line after 2)
    def _get_metadata_attr(self, this_line ):
        this_item = {}
        this_item['label'] = this_line['Metadata_label']
        this_item['value'] = this_line['Metadata_value']
        self.result_json['metadata'].append(this_item)

    # process data rows from sequence CSV to create pages within default sequence
    def _add_pages_to_sequence(self, this_line ):
        this_item={}
        this_item['file']= this_line['Filenames']
        this_item['label']= this_line['Label']
        self.result_json['sequences'][0]['pages'].append(this_item)

    # print out our constructed json
    def dumpJson(self):
        return json.dumps(self.result_json, indent=2)

    # Read in the CSV files. Note Here: this only works for csv UTF-8, so if these are being produced from
    # Excel sheets, we'll need to export them in that format

    #Read Main CSV file first - add to result_json['sequences'][0]['pages'] (for now, there is only one display sequence)
    # row 1 should be the headers, row 2 should have most of our metadata. Any row after this
    # is used only to provide global metadata

    def buildJson(self):
        with open(self.main_csv, 'r') as csv_file:
            reader = csv.DictReader(csv_file)
            for this_row in reader:
                if reader.line_num == 1:
                    #we can skip these
                    pass
                elif reader.line_num == 2:
                    self._get_attr_from_main_firstline( this_row)
                else:
                    self._get_metadata_attr(this_row)

         #Sequence CSV File next, add to pages 
        with open(self.sequence_csv, 'r') as sequence_file:
            reader = csv.DictReader(sequence_file)
            for this_row in reader:
                if reader.line_num == 1:
                    #we can skip these
                    pass
                else:
                    self._add_pages_to_sequence(this_row)
