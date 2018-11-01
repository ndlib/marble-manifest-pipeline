#!/usr/bin/env python

import json, csv, os

main_csv = "/Users/msuhovec/repos/mellon-manifest-pipeline/example/example-main.csv"
sequence_csv = "/Users/msuhovec/repos/mellon-manifest-pipeline/example/example-sequence.csv"

#start with an empty result json

result_json = {}
config = {}

#Start Function definitions
def get_config_param():
    global config
    config['server_url']='https://image-server.library.nd.edu:8182'
    config['path_prefix']='/iiif/2'

# set up framework of an empty results_json 
def set_json_skeleton( ):
    global result_json
    result_json['errors']=[]
    result_json['creator']='creator@email.com'
    result_json['metadata']=[]
    result_json['sequences']=[]
    result_json['sequences'].append({})
    result_json['sequences'][0]['pages']=[]

# process first data row of main CSV
def get_attr_from_main_firstline( first_line ):
    global result_json, config
    result_json['label'] = first_line['Label']
    result_json['description'] = first_line['Description']
    result_json['attribution'] = first_line['Attribution']
    result_json['rights'] = first_line['Rights']
    result_json['unique-identifier'] = first_line['unique_identifier']
    result_json['iiif-server'] = config['server_url'] + config['path_prefix']
    result_json['sequences'][0]['viewingHint'] = first_line['Sequence_viewing_experience']
    result_json['sequences'][0]['label'] = first_line['Sequence_label']

# process a metadata lable/value only row from the main CSV (any line after 2)
def get_metadata_attr( this_line ):
    global result_json
    this_item = {}
    this_item['label'] = this_line['Metadata_label']
    this_item['value'] = this_line['Metadata_value']
    result_json['metadata'].append(this_item)

# process data rows from sequence CSV to create pages within default sequence
def add_pages_to_sequence( this_line ):
    global result_json
    print(this_line)
    this_item={}
    this_item['file']= this_line['Filenames']
    this_item['label']= this_line['Label']
    result_json['sequences'][0]['pages'].append(this_item)



#start of main execution

# set up framework of empty json
get_config_param()
set_json_skeleton()

# Read in the CSV files. Note Here: this only works for csv UTF-8, so if these are being produced from
# Excel sheets, we'll need to export them in that format

#Read Main CSV file first - add to result_json['sequences'][0]['pages'] (for now, there is only one display sequence)
# row 1 should be the headers, row 2 should have most of our metadata. Any row after this
# is used only to provide global metadata

with open(main_csv, 'r') as csv_file:
    reader = csv.DictReader(csv_file)
    for this_row in reader:
        if reader.line_num == 1:
            #we can skip these
            pass
        elif reader.line_num == 2:
            get_attr_from_main_firstline( this_row)
        else:
            get_metadata_attr(this_row)

#Sequence CSV File next, add to pages 
with open(sequence_csv, 'r') as sequence_file:
    reader = csv.DictReader(sequence_file)
    for this_row in reader:
        if reader.line_num == 1:
            #we can skip these
            pass
        else:
            add_pages_to_sequence(this_row)

#print resulting json to STDOUT
print(json.dumps(result_json, indent=2))
