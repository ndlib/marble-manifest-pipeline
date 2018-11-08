import json

result_json = {}
config = {}
global_data = {}

#Start Function definitions
def get_config_param():
    global config
    config['input-base-url'] = '/Users/lrobins5/git/mellon-manifest-pipeline/example/'
    config['input-file-name'] = 'example-input.json'
    config['input-file'] = config['input-base-url'] + config['input-file-name']

    config['output-base-url'] = '/Users/lrobins5/git/mellon-manifest-pipeline/example/'
    config['output-file-suffix'] = '-manifest.json'

def load_global_data( input_json ):
    global global_data
    global_data['id-base'] = input_json['manifest-base-url'] + 'iiif' + input_json['unique-identifier'] + '/'
    global_data['output-file'] = config['output-base-url'] + input_json['unique-identifier'] + config['output-file-suffix']
    global_data['iiif-server'] = input_json['iiif-server']
    global_data['@context'] = 'http://iiif.io/api/presentation/2/context.json'
    global_data['default-height'] = 1000
    global_data['default-width'] = 1000
    global_data['viewingDirection'] = 'left-to-right'

# build results_json
def create_manifest_json( input_json ):
    global config
    global result_json
    global global_data
    load_global_data( input_json )

    result_json['@context'] = global_data['@context']
    result_json['@type'] = 'sc:Manifest'
    result_json['@id'] = global_data['id-base'] + 'manifest'
    result_json['label'] = input_json['label']
    result_json['metadata'] = input_json['metadata']
    result_json['description'] = input_json['description']
    result_json['license'] = input_json['rights']
    result_json['attribution'] = input_json['attribution']
    # currently, one sequence is allowed per manifest
    sequence_data = input_json['sequences'][0]
    add_sequence( sequence_data)

def add_sequence( sequence_data ):
    global result_json
    global global_data
    result_json['sequences'] = []

    this_item = {}
    this_item['@id'] = global_data['id-base'] + 'sequence/normal'
    this_item['@type'] = 'sc:Sequence'
    this_item['label'] = sequence_data['label']
    this_item['viewingDirection'] = global_data['viewingDirection']
    this_item['viewingHint'] = sequence_data['viewingHint']

    result_json['sequences'] = []
    result_json['sequences'].append(this_item)

    # each input page gets a new canvas and image
    if 'pages' in sequence_data:
        result_json['sequences'][0]['canvases'] = []
        i = 0
        for page_data in sequence_data['pages']:
            add_new_canvas_data(page_data, i)
            i = i + 1

def add_new_canvas_data( page_data, i ):
    global result_json
    global global_data
    this_item = {}
    this_item['@id'] = global_data['id-base'] + 'canvas/p' + str(i + 1)
    this_item['@type'] = 'sc:Canvas'
    this_item['label'] = page_data['label']
    this_item['height'] = global_data['default-height']
    this_item['width'] = global_data['default-width']
    result_json['sequences'][0]['canvases'].append(this_item)
    add_image_to_canvas(page_data, i)

def add_image_to_canvas( page_data, i ):
    global result_json
    global global_data
    result_json['sequences'][0]['canvases'][i]['images'] = []
    this_item = {}
    this_item['@id'] = global_data['id-base'] + 'images/' + page_data['file']
    this_item['@type'] = 'oa:Annotation'
    this_item['motivation'] = 'sc:painting'
    this_item['on'] = result_json['sequences'][0]['canvases'][i]['@id']
    result_json['sequences'][0]['canvases'][i]['images'].append(this_item)
    add_resource_to_image(page_data, i)

def add_resource_to_image( page_data, i ):
    global result_json
    global global_data
    result_json['sequences'][0]['canvases'][i]['images'][0]['resources'] = []
    this_item = {}
    this_item['@id'] = global_data['iiif-server'] + '/' + page_data['file'] + '/full/full/0/default.jpg'
    this_item['@type'] = 'dctypes:Image'
    this_item['format'] = 'image/jpeg'
    result_json['sequences'][0]['canvases'][i]['images'][0]['resources'].append(this_item)
    add_service_to_resource( i )

def add_service_to_resource(i):
    global result_json
    global global_data
    result_json['sequences'][0]['canvases'][i]['images'][0]['resources'][0]['service'] = []
    this_item = {}
    this_item['@id'] = global_data['id-base'] + 'images/'
    this_item['profile'] = "http://iiif.io/api/image/2/level2.json"
    result_json['sequences'][0]['canvases'][i]['images'][0]['resources'][0]['service'].append(this_item)

#start of main execution

get_config_param()

# read input json file
input_source = open(config['input-file'], 'r')
input_json = json.loads(input_source.read())
input_source.close()

create_manifest_json( input_json )

#print resulting json to STDOUT
print(json.dumps(result_json, indent=2))

# write data to manifest file

with open(global_data['output-file'], 'w') as output_file:
    json.dump(result_json, output_file, indent=2)
output_file.close()
