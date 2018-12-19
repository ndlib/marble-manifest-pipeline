import json, glob
import boto3


class processJson():
    def __init__(self):
        self.result_json = {}
        self.config = {}
        self.global_data = {}
        self.process_bucket = "manifestpipeline-dev-processbucket-1vtt3jhjtkg21"
        self.error = ''
        self.id = "example"

    def _set_file_data(self, inputDirectory, inputFile, outputDirectory):
        self.input_file = inputDirectory + inputFile

        self.output_base_url = outputDirectory
        self.output_file_suffix = '-manifest.json'

    def _load_global_data( self, input_json ):
        self.global_data['id-base'] = input_json['manifest-base-url'] + 'iiif' + '/' + input_json['unique-identifier'] + '/'
        self.global_data['output-file'] = self.output_base_url + input_json['unique-identifier'] + self.output_file_suffix
        self.global_data['iiif-server'] = input_json['iiif-server']
        self.global_data['@context'] = 'http://iiif.io/api/presentation/2/context.json'
        self.global_data['default-height'] = 2000
        self.global_data['default-width'] = 2000
        self.global_data['viewingDirection'] = 'left-to-right'

    # build results_json
    def _create_manifest_json( self, input_json ):
        self._load_global_data( input_json )

        self.result_json['@context'] = self.global_data['@context']
        self.result_json['@type'] = 'sc:Manifest'
        self.result_json['@id'] = self.global_data['id-base'] + 'manifest'
        self.result_json['label'] = input_json['label']
        self.result_json['metadata'] = input_json['metadata']
        self.result_json['description'] = input_json['description']
        self.result_json['license'] = input_json['rights']
        self.result_json['attribution'] = input_json['attribution']
        # currently, one sequence is allowed per manifest
        sequence_data = input_json['sequences'][0]
        self._add_sequence( sequence_data)

    def _add_sequence( self, sequence_data ):
        self.result_json['sequences'] = []

        this_item = {}
        this_item['@id'] = self.global_data['id-base'] + 'sequence/normal'
        this_item['@type'] = 'sc:Sequence'
        this_item['label'] = sequence_data['label']
        this_item['viewingDirection'] = self.global_data['viewingDirection']
        this_item['viewingHint'] = sequence_data['viewingHint']

        self.result_json['sequences'] = []
        self.result_json['sequences'].append(this_item)

        # each input page gets a new canvas and image
        if 'pages' in sequence_data:
            self.result_json['sequences'][0]['canvases'] = []
            i = 0
            for page_data in sequence_data['pages']:
                self._add_new_canvas_data(page_data, i)
                i = i + 1

    def _add_new_canvas_data( self, page_data, i ):
        this_item = {}
        this_item['@id'] = self.global_data['id-base'] + 'canvas/p' + str(i + 1)
        this_item['@type'] = 'sc:Canvas'
        this_item['label'] = page_data['label']
        this_item['height'] = self.global_data['default-height']
        this_item['width'] = self.global_data['default-width']
        self.result_json['sequences'][0]['canvases'].append(this_item)
        self._add_image_to_canvas(page_data, i)

    def _add_image_to_canvas( self, page_data, i ):
        self.result_json['sequences'][0]['canvases'][i]['images'] = []
        this_item = {}
        this_item['@id'] = self.global_data['id-base'] + 'images/' + page_data['file']
        this_item['@type'] = 'oa:Annotation'
        this_item['motivation'] = 'sc:painting'
        this_item['on'] = self.result_json['sequences'][0]['canvases'][i]['@id']
        self.result_json['sequences'][0]['canvases'][i]['images'].append(this_item)
        self._add_resource_to_image(page_data, i)

    def _add_resource_to_image( self, page_data, i ):
        this_item = {}
        this_item['@id'] = self.global_data['iiif-server'] + '/' + page_data['file'] + '/full/full/0/default.jpg'
        this_item['@type'] = 'dctypes:Image'
        this_item['format'] = 'image/jpeg'
        self.result_json['sequences'][0]['canvases'][i]['images'][0]['resource'] = this_item
        self._add_service_to_resource(page_data, i)

    def _add_service_to_resource( self, page_data, i ):
        this_item = {}
        this_item['@id'] = self.global_data['iiif-server'] + '/' + page_data['file']
        this_item['profile'] = "http://iiif.io/api/image/2/level2.json"
        this_item['@context'] = "http://iiif.io/api/image/2/context.json"
        self.result_json['sequences'][0]['canvases'][i]['images'][0]['resource']['service'] = this_item

    # returns True if input file found, false otherwise
    def verifyInput(self, inputDirectory, inputFile, outputDirectory):
        self._set_file_data(inputDirectory, inputFile, outputDirectory)
        main_match = glob.glob(self.input_file)
        if not main_match:
            self.error = 'Error: ' + self.input_file + ' not found'
            return False
        return True

    # returns error string
    def errorInputRequired(self):
        self.error = 'Input directory and file name are required.'
        return self.error

    # start of main execution
    def buildManifest(self):
        with open(self.input_file, 'r') as input_source:
            input_json = json.loads(input_source.read())
        input_source.close()
        self._create_manifest_json(input_json)

    #print resulting json to STDOUT
    def printManifest(self):
        print(json.dumps(self.result_json, indent=2))

    # write data to manifest json file
    def dumpManifest(self):
        s3 = boto3.resource('s3')
        key = "finished/" + self.id + "/manifest.json"
        #k.content_type = "application/json+ld"
        #k.set_contents_from_string(json.dumps(self.result_json))

        s3.Object(self.process_bucket, key).put(Body=json.dumps(self.result_json))
