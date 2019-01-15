import json, csv, os, glob
import boto3
from botocore.errorfactory import ClientError

class processCsv():
    # class constructor
    def __init__(self, id, process_bucket, manifest_bucket, image_bucket):
        self.id = id
        self.process_bucket = process_bucket
        self.manifest_bucket = manifest_bucket
        self.image_bucket = image_bucket
        self.error = []
        #start with an empty result json and config
        self.result_json = {}
        self.config = {}
        # get config information
        self._get_config_param()
        # population json info that is not csv-dependent
        self._set_json_skeleton()


    def _get_config_param(self):
        # get these from wherever
        self.config['image-server-base-url']='https://image-server.library.nd.edu:8182/iiif/2'
        self.config["manifest-server-base-url"] = "https://manifest.nd.edu"
        self.config['process-bucket'] = self.process_bucket
        self.config['process-bucket-read-basepath'] = 'process'
        self.config['process-bucket-write-basepath'] = 'finished'
        self.config['image-server-bucket'] = self.image_bucket
        self.config['image-server-bucket-basepath'] = ''
        self.config['manifest-server-bucket'] = self.manifest_bucket
        self.config['manifest-server-bucket-basepath'] = ''
        self.config['sequence-csv'] = 'sequence.csv'
        self.config['main-csv'] = 'main.csv'
        self.config['canvas-default-height'] = 2000
        self.config['canvas-default-width'] = 2000
        self.config["notify-on-finished"] = "notify@email.com"

    # set up framework of an empty results_json
    def _set_json_skeleton(self):
        self.result_json['errors']=[]
        self.result_json['creator']='creator@email.com'
        self.result_json['viewingDirection']='left-to-right'
        self.result_json['config'] = self.config
        self.result_json['metadata']=[]
        self.result_json['sequences']=[]
        self.result_json['sequences'].append({})
        self.result_json['sequences'][0]['pages']=[]

    # returns True if main and sequence csv fles found, false otherwise
    def verifyCsvExist(self,  csvDirectory = '.'):
        s3 = boto3.client('s3')

        try:
            s3.head_object(Bucket=self.config['process-bucket'], Key=self.config['process-bucket-read-basepath'] + "/" + self.id + "/" + self.config['main-csv'])
        except ClientError as err:
            self.error.append(err)
            pass

        try:
            s3.head_object(Bucket=self.config['process-bucket'], Key=self.config['process-bucket-read-basepath'] + "/" + self.id + "/" + self.config['sequence-csv'])
        except ClientError as err:
            self.error.append(err)
            pass

        return (len(self.error) == 0)


    # returns error string
    def printCsvError(self):
        return self.error

    # process first data row of main CSV
    def _get_attr_from_main_firstline( self, first_line ):
        self.result_json['label'] = first_line['Label']
        self.result_json['description'] = first_line['Description']
        self.result_json['attribution'] = first_line['Attribution']
        self.result_json['rights'] = first_line['Rights']
        self.result_json['unique-identifier'] = first_line['unique_identifier']
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
        s3 = boto3.resource('s3')
        obj = s3.Object(self.config['process-bucket'], self.config['process-bucket-read-basepath'] + "/" + self.id + "/" + self.config['main-csv']).download_file('/tmp/' + self.config['main-csv'])

        with open('/tmp/' + self.config['main-csv'], 'r') as csv_file:
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
        obj = s3.Object(self.config['process-bucket'], self.config['process-bucket-read-basepath'] + "/" + self.id + "/" + self.config['sequence-csv']).download_file('/tmp/' + self.config['sequence-csv'])

        with open('/tmp/' + self.config['sequence-csv'], 'r') as sequence_file:
            reader = csv.DictReader(sequence_file)
            for this_row in reader:
                if reader.line_num == 1:
                    #we can skip these
                    pass
                else:
                    self._add_pages_to_sequence(this_row)
