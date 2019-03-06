import json
import glob
import os
import boto3


class processJson():
    def __init__(self, id, eventConfig):
        self.result_json = {}
        self.config = eventConfig
        self.global_data = self.readEventData(id)
        self.error = []
        self.id = id

    def _set_file_data(self, inputDirectory, inputFile, outputDirectory):
        self.input_file = inputDirectory + inputFile

        self.output_base_url = outputDirectory
        self.output_file_suffix = '-manifest.json'

    def _load_global_data(self):
        self.global_data['id-base'] = self.config['manifest-server-base-url'] + '/' + self.id + '/'

    # build results_json
    def _create_manifest_json(self):
        self._load_global_data()

        self.result_json['@context'] = 'http://iiif.io/api/presentation/2/context.json'
        self.result_json['@type'] = 'sc:Manifest'
        self.result_json['@id'] = self.global_data['id-base'] + 'manifest'
        self.result_json['label'] = self.global_data['label']
        self.result_json['metadata'] = self.global_data['metadata']
        self.result_json['description'] = self.global_data['description']
        self.result_json['license'] = self.global_data['license']
        self.result_json['attribution'] = self.global_data['attribution']

        # currently, one sequence is allowed per manifest
        sequence_data = self.global_data['sequences'][0]
        file_name = sequence_data['pages'][0]['file']
        self.result_json['thumbnail'] = self._add_thumbnail(file_name)

        self._add_sequence( sequence_data)

    def _add_thumbnail(self, file_name):
        file_name = self.filename_without_extension(file_name) + ".tif"
        thumbnail = {}
        thumbnail['@id'] = self.config['image-server-base-url'] + '/' + self.id + '%2F' \
            + file_name + '/full/250,/0/default.jpg'
        thumbnail['service'] = {
            '@id': self.config['image-server-base-url'] + '/' + self.id + '%2F' + file_name,
            'profile': "http://iiif.io/api/image/2/level2.json",
            '@context': "http://iiif.io/api/image/2/context.json"
        }
        thumbnail['@context'] = "http://iiif.io/api/image/2/context.json"
        thumbnail['profile'] = "http://iiif.io/api/image/2/level1.json"
        return thumbnail

    def _add_sequence(self, sequence_data):
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

    def _add_new_canvas_data(self, page_data, i):
        this_item = {}
        this_item['@id'] = self.global_data['id-base'] + 'canvas/p' + str(i + 1)
        this_item['@type'] = 'sc:Canvas'
        this_item['label'] = page_data['label']
        this_item['height'] = self.config['canvas-default-height']
        this_item['width'] = self.config['canvas-default-width']
        self.result_json['sequences'][0]['canvases'].append(this_item)

        file_name = page_data['file']
        this_item['thumbnail'] = self._add_thumbnail(file_name)
        self._add_image_to_canvas(page_data, i)

    def _add_image_to_canvas(self, page_data, i):
        self.result_json['sequences'][0]['canvases'][i]['images'] = []
        this_item = {}
        this_item['@id'] = self.global_data['id-base'] + 'images/' + page_data['file']
        this_item['@type'] = 'oa:Annotation'
        this_item['motivation'] = 'sc:painting'
        this_item['on'] = self.result_json['sequences'][0]['canvases'][i]['@id']
        self.result_json['sequences'][0]['canvases'][i]['images'].append(this_item)
        self._add_resource_to_image(page_data, i)

    def _add_resource_to_image(self, page_data, i):
        this_item = {}
        file = self.filename_without_extension(page_data['file']) + ".tif"
        this_item['@id'] = self.config['image-server-base-url'] + '/' \
            + self.id + '%2F' + file + '/full/full/0/default.jpg'
        this_item['@type'] = 'dctypes:Image'
        this_item['format'] = 'image/jpeg'
        self.result_json['sequences'][0]['canvases'][i]['images'][0]['resource'] = this_item
        self._add_service_to_resource(page_data, i)

    def _add_service_to_resource(self, page_data, i):
        this_item = {}
        file = self.filename_without_extension(page_data['file']) + ".tif"
        this_item['@id'] = self.config['image-server-base-url'] + '/' + self.id + '%2F' + file
        this_item['profile'] = "http://iiif.io/api/image/2/level2.json"
        this_item['@context'] = "http://iiif.io/api/image/2/context.json"
        self.result_json['sequences'][0]['canvases'][i]['images'][0]['resource']['service'] = this_item

    # returns True if input file found, false otherwise
    def verifyInput(self, inputDirectory, inputFile, outputDirectory):
        self._set_file_data(inputDirectory, inputFile, outputDirectory)
        main_match = glob.glob(self.input_file)
        if not main_match:
            self.error.append('Error: ' + self.input_file + ' not found')
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

    # print resulting json to STDOUT
    def printManifest(self):
        print(json.dumps(self.result_json, indent=2))

    def _write_json_s3(self, key, data):
        s3 = boto3.resource('s3')
        s3.Object(self.config["process-bucket"], key).put(Body=json.dumps(data), ContentType='text/json')

    # write data to manifest json file
    def dumpManifest(self):
        key = self.config["process-bucket-write-basepath"] + "/" + self.id + "/manifest/index.json"
        self._write_json_s3(key, self.result_json)

    # store event data
    def writeEventData(self, event):
        key = self.config['process-bucket-read-basepath'] + "/" + self.id + "/" + self.config["event-file"]
        self._write_json_s3(key, event)

    # read event data
    def readEventData(self, event_id):
        remote_file = self.config['process-bucket-read-basepath'] + "/" + event_id + "/" + self.config["event-file"]
        content_object = boto3.resource('s3').Object(self.config['process-bucket'], remote_file)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        return json.loads(file_content).get('data')

    def filename_without_extension(self, file):
        return os.path.splitext(file)[0]
