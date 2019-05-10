import json
import csv
from io import StringIO


class processCsv():
    # class constructor
    def __init__(self, id, eventConfig, main_csv, items_csv):
        self.id = id
        self.error = []
        # start with an empty result json and config
        self.result_json = {}
        self.config = eventConfig
        self.main_csv = main_csv
        self.items_csv = items_csv
        # population json info that is not csv-dependent
        self._set_json_skeleton()
        self.lang = "en"

    # set up framework of an empty results_json
    def _set_json_skeleton(self):
        self.result_json['errors'] = []
        self.result_json['creator'] = 'creator@email.com'
        self.result_json['viewingDirection'] = 'left-to-right'
        self.result_json['metadata'] = []
        self.result_json['items'] = []

    # process first data row of main CSV
    def _get_attr_from_main_firstline(self, first_line):
        self.result_json['label'] = self._lang_wrapper(first_line['Label'])
        self.result_json['requiredStatement'] = self._get_requiredstatement(first_line['Attribution'])
        self.result_json['rights'] = first_line['Rights']
        self.result_json['unique-identifier'] = first_line['unique_identifier']
        self._get_alternate_attr(first_line)
        self._get_summary(first_line['Summary'])
        self._get_metadata_attr(first_line)
        self._get_seealso_attr(first_line)
        self.config['index-marble'] = True
        if 'Index_for_MARBLE' in first_line:
            if first_line['Index_for_MARBLE']:
                self.config['index-marble'] = False
        self.config['notify-on-finished'] = first_line['Notify']

    # process metadata columns from the main CSV
    def _get_metadata_attr(self, this_line):
        if this_line['Metadata_label'] and this_line['Metadata_value']:
            this_item = {}
            this_item.update(self._label_wrapper(this_line['Metadata_label']))
            this_item.update(self._value_wrapper(this_line['Metadata_value']))
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
                this_item['label'] = {"en": [this_line['Alternate_id_system'] + " - " + self.id]}
                this_item['type'] = "Text"
                this_item['format'] = "text/html"
                if 'homepage' not in self.result_json:
                    self.result_json['homepage'] = []
                self.result_json['homepage'].append(this_item)

    # process seealso columns from the main CSV
    def _get_seealso_attr(self, this_line):
        seealso_keys = ('SeeAlso_Id', 'SeeAlso_Type', 'SeeAlso_Format', 'SeeAlso_Label', 'SeeAlso_Profile')
        # check if all the seealso keys exist
        if all(sa_key in this_line for sa_key in seealso_keys):
            # check to see if we have data in that column
            if this_line['SeeAlso_Id']:
                this_item = {}
                this_item['id'] = this_line['SeeAlso_Id']
                this_item['type'] = this_line['SeeAlso_Type']
                this_item['format'] = this_line['SeeAlso_Format']
                this_item['profile'] = this_line['SeeAlso_Profile']
                if 'seeAlso' not in self.result_json:
                    self.result_json['seeAlso'] = []
                self.result_json['seeAlso'].append(this_item)

    # process data rows from items CSV
    def _add_items(self, this_line):
        if this_line['Filenames'] and this_line['Label']:
            this_item = {}
            this_item['file'] = this_line['Filenames']
            this_item['label'] = this_line['Label']
            self.result_json['items'].append(this_item)

    def _get_requiredstatement(self, this_line):
        rs = {}
        rs.update(self._label_wrapper("Attribution"))
        rs.update(self._value_wrapper(this_line))
        return rs

    def _get_summary(self, this_line):
        summary = {}
        summary.update(self._label_wrapper("Summary"))
        summary.update(self._value_wrapper(this_line))
        self.result_json['metadata'].append(summary)

    def _lang_wrapper(self, line):
        return {self.lang: [line]}

    def _label_wrapper(self, line):
        return {"label": self._lang_wrapper(line)}

    def _value_wrapper(self, line):
        return {"value": self._lang_wrapper(line)}

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
        f = StringIO(self.main_csv)
        reader = csv.DictReader(f, delimiter=',')
        for this_row in reader:
            if reader.line_num == 2:
                self._get_attr_from_main_firstline(this_row)
            elif reader.line_num > 2:
                self._get_metadata_attr(this_row)
                self._get_seealso_attr(this_row)
                self._get_alternate_attr(this_row)

        # Items CSV File next, add to pages
        f = StringIO(self.items_csv)
        reader = csv.DictReader(f, delimiter=',')
        for this_row in reader:
            if reader.line_num == 2:
                self.config['default-img'] = this_row['Filenames']
            if reader.line_num != 1:
                self._add_items(this_row)
