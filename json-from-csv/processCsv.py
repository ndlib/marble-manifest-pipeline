import json, csv, os, glob, io
import boto3
from botocore.errorfactory import ClientError

class processCsv():
    # class constructor
    def __init__(self, id, eventConfig):
        self.id = id
        self.error = []
        #start with an empty result json and config
        self.result_json = {}
        self.config = eventConfig
        # population json info that is not csv-dependent
        self._set_json_skeleton()


    # set up framework of an empty results_json
    def _set_json_skeleton(self):
        self.result_json['errors']=[]
        self.result_json['creator']='creator@email.com'
        self.result_json['viewingDirection']='left-to-right'
        self.result_json['metadata']=[]
        self.result_json['sequences']=[]
        self.result_json['sequences'].append({})
        self.result_json['sequences'][0]['pages']=[]

    # returns True if main and sequence csv fles found, false otherwise
    def verifyCsvExist(self):
        s3 = boto3.client('s3')
        try:
            s3.head_object(Bucket=self.config['process-bucket'], Key=self.config['process-bucket-read-basepath'] + "/" + self.id + "/" + self.config['main-csv'])
        except ClientError as err:
            self.error.append("main.csv does not exist in the process bucket for id, " + self.id)
            pass

        try:
            s3.head_object(Bucket=self.config['process-bucket'], Key=self.config['process-bucket-read-basepath'] + "/" + self.id + "/" + self.config['sequence-csv'])
        except ClientError as err:
            self.error.append("sequence.csv does not exist in the process bucket for id, " + self.id)
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
        self.result_json['license'] = first_line['License']
        self.result_json['unique-identifier'] = first_line['unique_identifier']
        self.result_json['sequences'][0]['viewingHint'] = first_line['Sequence_viewing_experience']
        self.result_json['sequences'][0]['label'] = first_line['Sequence_label']

    # process a metadata lable/value only row from the main CSV (any line after 2)
    def _get_metadata_attr(self, this_line ):
        if this_line['Metadata_label'] and this_line['Metadata_value']:
            this_item = {}
            this_item['label'] = this_line['Metadata_label']
            this_item['value'] = this_line['Metadata_value']
            self.result_json['metadata'].append(this_item)

    # process data rows from sequence CSV to create pages within default sequence
    def _add_pages_to_sequence(self, this_line ):
        if this_line['Filenames'] and this_line['Label']:
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
        s3.Object(self.config['process-bucket'], self.config['process-bucket-read-basepath'] + "/" + self.id + "/" + self.config['main-csv']).download_file('/tmp/' + self.config['main-csv'])

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
        s3.Object(self.config['process-bucket'], self.config['process-bucket-read-basepath'] + "/" + self.id + "/" + self.config['sequence-csv']).download_file('/tmp/' + self.config['sequence-csv'])

        with open('/tmp/' + self.config['sequence-csv'], 'r') as sequence_file:
            reader = csv.DictReader(sequence_file)
            for this_row in reader:
                if reader.line_num == 1:
                    #we can skip these
                    pass
                else:
                    self._add_pages_to_sequence(this_row)

    # store event data
    def writeEventData(self, event):
        local_file = '/tmp/' + self.config["event-file"]
        with io.open(local_file, 'w', encoding='utf-8') as f:
            f.write(json.dumps(event, ensure_ascii=False))
        s3_file = self.config['process-bucket-read-basepath'] + "/" + self.id + "/" + self.config["event-file"]
        boto3.resource('s3').Bucket(self.config['process-bucket']).upload_file(local_file, s3_file)
        os.remove(local_file)

    # read event data
    def readEventData(self, filename):
        remote_file = self.config['process-bucket-read-basepath'] + "/" + self.id + "/" + self.config["event-file"]
        content_object = boto3.resource('s3').Object(self.config['process-bucket'], remote_file)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        return json.loads(file_content)
