import json
import csv
import os
from io import StringIO


class processCsv():
    # class constructor
    def __init__(self, config, main_csv, items_csv, image_data):
        self.id = config['id']
        self.error = []
        # start with an empty result json and config
        self.result_json = {}
        self.config = config
        self.main_csv = main_csv
        self.items_csv = items_csv
        self.image_data = image_data
        # population json info that is not csv-dependent
        self._set_json_skeleton()
        self.lang = "en"

    # set up framework of an empty results_json
    def _set_json_skeleton(self):
        self.result_json['id'] = self.id
        self.result_json['version'] = '1.0'
        self.result_json['errors'] = []
        self.result_json['creator'] = 'creator@email.com'
        self.result_json['manifest-type'] = 'manifest'
        self.result_json['language'] = 'en'
        self.result_json['source-system'] = ''
        self.result_json['items'] = []

    # process first data row of main CSV
    def _get_attr_from_main_firstline(self, first_line):
        self.result_json['label'] = first_line['Label']
        if first_line.get('Attribution', False):
            self.result_json['requiredStatement'] = self._get_requiredstatement(first_line['Attribution'])
        if first_line.get('Rights', False):
            self.result_json['rights'] = first_line['Rights']
        if first_line.get('Summary', False):
            self.result_json['summary'] = first_line['Summary']
        self._get_alternate_attr(first_line)
        self._get_metadata_attr(first_line)
        self._get_seealso_attr(first_line)
        self._get_provider(first_line)

        if ('thumbnail' not in self.result_json):
            self.result_json['thumbnail'] = self.result_json['items'][0]['file']

        self.config['index-marble'] = True
        if 'Index_for_MARBLE' in first_line:
            if first_line['Index_for_MARBLE']:
                self.config['index-marble'] = False
        self.config['notify-on-finished'] = first_line['Notify']

    def _get_provider(self, first_line):
        if first_line.get('Alternate_id_system', False):
            self.result_json['provider'] = [first_line['Alternate_id_system']]

    # process metadata columns from the main CSV
    def _get_metadata_attr(self, this_line):
        if this_line['Metadata_label'] and this_line['Metadata_value']:
            this_item = {
                "label": this_line['Metadata_label'],
                "value": this_line['Metadata_value'],
            }
            # get itself or set it to []
            self.result_json['metadata'] = self.result_json.get('metadata', [])
            self.result_json['metadata'].append(this_item)

    # process alternate columns from the main CSV
    def _get_alternate_attr(self, this_line):
        alternate_keys = ('Alternate_id_system', 'Alternate_id_identifier', 'Alternate_id_url')
        # check if all the alternate keys exist
        if all(alt_key in this_line for alt_key in alternate_keys):
            # check to see if we have data in that column
            if this_line['Alternate_id_url']:
                this_item = {}
                this_item['id'] = this_line['Alternate_id_url']
                this_item['label'] = this_line['label']
                this_item['type'] = "HTML"
                this_item['format'] = "text/html"

                self.result_json['seeAlso'] = self.result_json.get('seeAlso', [])
                self.result_json['seeAlso'].append(this_item)

    # process seealso columns from the main CSV
    def _get_seealso_attr(self, this_line):
        seealso_keys = ('SeeAlso_Id', 'SeeAlso_Type', 'SeeAlso_Format', 'SeeAlso_Label', 'SeeAlso_Profile')
        # check if all the seealso keys exist
        if all(sa_key in this_line for sa_key in seealso_keys):
            # check to see if we have data in that column
            if this_line['SeeAlso_Id']:
                this_item = {}
                this_item['id'] = this_line['SeeAlso_Id']
                this_item['label'] = this_line['SeeAlso_Label']
                this_item['type'] = this_line['SeeAlso_Type']
                this_item['format'] = this_line['SeeAlso_Format']
                this_item['profile'] = this_line['SeeAlso_Profile']

                self.result_json['seeAlso'] = [this_item]

    # process data rows from items CSV
    def _add_items(self, this_line):
        if this_line['Filenames']:
            this_item = {}
            this_item['file'] = this_line['Filenames']
            if ('Label' in this_line and this_line['Label']):
                this_item['label'] = this_line['Label']
            if ('Description' in this_line and this_line['Description']):
                this_item['summary'] = this_line['Description']
            if ('DefaultImage' in this_line and this_line['DefaultImage']):
                self.result_json['thumbnail'] = this_line['Filenames']

            this_item['height'] = self._get_canvas_height(this_line['Filenames'])
            this_item['width'] = self._get_canvas_width(this_line['Filenames'])
            this_item['manifest-type'] = 'image'

            self.result_json['items'].append(this_item)

    def _get_canvas_height(self, filename):
        # filename without extension
        base_filename = os.path.splitext(filename)[0]
        height = self.config['canvas-default-height']
        if self.image_data.get(base_filename):
            height = self.image_data.get(base_filename)['height']
        return height

    def _get_canvas_width(self, filename):
        # filename without extension
        base_filename = os.path.splitext(filename)[0]
        width = self.config['canvas-default-width']
        if self.image_data.get(base_filename):
            width = self.image_data.get(base_filename)['width']
        return width

    def _get_requiredstatement(self, this_line):
        rs = {}
        rs.update(self._label_wrapper("Attribution"))
        rs.update(self._value_wrapper(this_line))
        return rs

    def _label_wrapper(self, line):
        return {"label": line}

    def _value_wrapper(self, line):
        return {"value": line}

    # print out our constructed json
    def dumpJson(self):
        return json.dumps(self.result_json, indent=2)

    # Read in the CSV files. Note Here: this only works for csv UTF-8, so if these are being produced from
    # Excel sheets, we'll need to export them in that format

    # Read Main CSV file first
    # add to result_json['items']
    # row 1 should be the headers, row 2 should have most of our metadata.
    # Any row after this is used only to provide global metadata
    def buildJson(self):
        # Items CSV File next, add to pages
        f = StringIO(self.items_csv)
        reader = csv.DictReader(f, delimiter=',')
        for this_row in reader:
            if reader.line_num != 1:
                self._add_items(this_row)

        f = StringIO(self.main_csv)
        reader = csv.DictReader(f, delimiter=',')
        for this_row in reader:
            if reader.line_num == 2:
                self._get_attr_from_main_firstline(this_row)
            elif reader.line_num > 2:
                self._get_metadata_attr(this_row)
                self._get_seealso_attr(this_row)
                self._get_alternate_attr(this_row)
