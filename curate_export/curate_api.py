"""
Curate_API
Calls the Curate API, massages results to create standard json output, which is then saved.
"""

import time
import json
from datetime import datetime
import os
import io
from translate_curate_json_node import TranslateCurateJsonNode
from create_standard_json import CreateStandardJson
from pipelineutilities.standard_json_helpers import StandardJsonHelpers
from pipelineutilities.save_standard_json_to_dynamo import SaveStandardJsonToDynamo
from pipelineutilities.save_standard_json import save_standard_json
from save_json_to_dynamo import SaveJsonToDynamo
from dynamo_helpers import add_file_keys
from dynamo_query_functions import get_item_record, get_file_record
from dynamo_save_functions import save_file_group_record, save_file_to_process_record
from record_files_needing_processed import FilesNeedingProcessed
from s3_helpers import read_s3_json, write_s3_json
from get_curate_metadata import GetCurateMetadata


class CurateApi():
    """ This performs all Curate API processing """
    def __init__(self, config, event, time_to_break):
        self.config = config
        self.event = event
        self.start_time = time.time()
        if not self.config.get('local', True):
            self.curate_header = {"X-Api-Token": self.config["curate-token"]}
        self.start_time = time.time()
        self.time_to_break = time_to_break
        self.translate_curate_json_node_class = TranslateCurateJsonNode(config)
        self.save_standard_json_locally = True  # event.get("local", False)
        self.create_standard_json_class = CreateStandardJson(config)
        self.local_folder = os.path.dirname(os.path.realpath(__file__)) + "/"
        self.save_json_to_dynamo_class = SaveJsonToDynamo(config, self.config.get('website-metadata-tablename', ''))
        self.get_curate_metadata_class = GetCurateMetadata(config, event, time_to_break)

    def process_curate_items(self, ids: list) -> bool:
        """ Given a list of ids, process each one that corresponds to a Curate item """
        aborted_processing = False
        for item_id in list(ids):  # iterate over a copy of the list, so we can remove items from the original list
            if "und:" in item_id:
                curate_id = item_id.replace("und:", "")  # strip namespace
                if 'itemBeingProcessed' not in self.event:
                    self.event['itemBeingProcessed'] = {'id': curate_id}
                self.process_curate_item(curate_id)
                if 'itemBeingProcessed' not in self.event:
                    ids.remove(item_id)  # if we successfully completed processing this item, remove it from list to be processed.
            if datetime.now() > self.time_to_break and len(ids) > 0:
                aborted_processing = True
                break
        return not aborted_processing

    def process_curate_item(self, item_id: str) -> dict:  # noqa: C901
        """ Get json metadata for a curate item given an item_id
            Note: query is of the form: curate-server-base-url + "/api/items/<pid>" """
        standard_json = {}
        print("starting to process item", item_id, 'after', int(time.time() - self.start_time), 'seconds')
        curate_json = self.get_curate_metadata_class.get_curate_json(item_id)
        self.event['itemBeingProcessed']['gotCurateJson'] = True
        standard_json = self._get_standard_json(curate_json)
        self.event['itemBeingProcessed']['gotStandardJson'] = True
        if standard_json:
            if self.save_standard_json_locally:
                with open(self.local_folder + "test/" + item_id + "_standard.json", "w") as output_file:
                    json.dump(standard_json, output_file, indent=2, ensure_ascii=False)
            else:
                export_all_files_flag = self.event.get('export_all_files_flag', False)
                save_required_flag = self._save_standard_json_to_dynamo_required(standard_json)
                if save_required_flag and datetime.now() < self.time_to_break and not self.event['itemBeingProcessed'].get('savedStandardJsonToS3', False):
                    print("saving standard_json to S3 for", item_id, 'after', int(time.time() - self.start_time), 'seconds')
                    save_standard_json(self.config, standard_json)
                    self.event['itemBeingProcessed']['savedStandardJsonToS3'] = True
                if save_required_flag and datetime.now() < self.time_to_break and not self.event['itemBeingProcessed'].get('savedStandardJsonToDynamo', False):
                    save_standard_json_to_dynamo_class = SaveStandardJsonToDynamo(self.config, self.time_to_break)
                    print("saving standard_json to dynamo recursively for", item_id, 'after', int(time.time() - self.start_time), 'seconds')
                    save_standard_json_to_dynamo_class.save_standard_json(standard_json)
                    if datetime.now() < self.time_to_break:
                        self.event['itemBeingProcessed']['savedStandardJsonToDynamo'] = True
                if datetime.now() < self.time_to_break and not self.event['itemBeingProcessed'].get('savedFilesNeedingProcessedToDynamo', False):
                    # first_file_json = self._find_first_file_in_standard_json(standard_json)
                    # save_file_required_flag = self._save_file_to_dynamo_required(first_file_json)  # TODO: Generalize instead of assuming if the first file is saved, all are saved
                    if (save_required_flag or export_all_files_flag):  # or save_file_required_flag)
                        print("saving curate image data recursively to dynamo for", item_id)
                        file_needed_updated = self._save_curate_image_data_to_dynamo(standard_json, export_all_files_flag)
                        if datetime.now() < self.time_to_break:
                            self.event['itemBeingProcessed']['savedFilesNeedingProcessedToDynamo'] = True
                            if file_needed_updated:
                                print("updating files needing processed for", item_id, 'after', int(time.time() - self.start_time), 'seconds')
                                files_needing_processed_class = FilesNeedingProcessed(self.config)
                                files_needing_processed_class.record_files_needing_processed(standard_json, True)
                            self.event.pop('itemBeingProcessed', None)
                if datetime.now() < self.time_to_break:
                    self.event.pop('itemBeingProcessed', None)
                print('finished processing', item_id, 'after', int(time.time() - self.start_time), 'seconds')
        return standard_json

    def _get_standard_json(self, curate_json: dict) -> dict:
        """ Decide whether to use saved standard_json or whether to create it ourselves from curate_json"""
        item_id = curate_json.get('id')
        if self._generate_new_standard_json_required(curate_json):
            print("generating new standard_json")
            standard_json = self.translate_curate_json_node_class.build_json_from_curate_json(curate_json, "root", {})
            if self.save_standard_json_locally:
                with open(self.local_folder + "test/" + item_id + "_preliminary_standard.json", "w") as output_file:
                    json.dump(standard_json, output_file, indent=2, ensure_ascii=False)
            members = curate_json.get('members', [])
            standard_json = self.create_standard_json_class.build_standard_json_tree(standard_json, members)
            standard_json_helpers_class = StandardJsonHelpers(self.config)
            standard_json = standard_json_helpers_class.enhance_standard_json(standard_json)
            self._save_standard_json_for_future_processing(standard_json)
        else:
            print('using saved standard.json')
            standard_json = self._get_saved_standard_json(item_id)
        return standard_json

    def _get_saved_standard_json(self, item_id: str) -> dict:
        """ If standard.json has already been stored, read that
            If not running locally, check S3.  Else, check locally.  """
        standard_json = {}
        if self.config.get('local', True):
            filename = os.path.join(self.local_folder, "save", item_id + "_standard.json")
            if os.path.exists(filename):
                with io.open(filename, 'r', encoding='utf-8') as json_file:
                    standard_json = json.load(json_file)
        else:
            key = os.path.join('save', item_id + '_standard.json')
            standard_json = read_s3_json(self.config['process-bucket'], key)
        return standard_json

    def _save_standard_json_for_future_processing(self, standard_json: dict):
        """ Once we get standard_json, save it so we can process more easily next time. """
        item_id = standard_json.get('id', '')
        if self.config.get('local', True) or self.save_standard_json_locally:
            filename = os.path.join(self.local_folder, "save", item_id + "_standard.json")
            with open(filename, 'w') as f:
                json.dump(standard_json, f, indent=2)
        else:
            key = os.path.join('save', item_id + '_standard.json')
            write_s3_json(self.config['process-bucket'], key, standard_json)

    def _save_curate_image_data_to_dynamo(self, standard_json: dict, export_all_files_flag: False, file_needed_updated: bool = False):
        """ Save Curate image data to dynamo recursively """
        if standard_json.get('level', '') == 'file':
            new_dict = {i: standard_json[i] for i in standard_json if i != 'items'}
            if not new_dict.get('id', '').lower().endswith('.xml'):
                new_dict['objectFileGroupId'] = new_dict['parentId']
                new_dict = add_file_keys(new_dict)
                record_inserted_flag = self.save_json_to_dynamo_class.save_json_to_dynamo(new_dict, False)
                if record_inserted_flag or export_all_files_flag:
                    file_needed_updated = True
                    save_file_to_process_record(self.config['website-metadata-tablename'], new_dict, False)
                    save_file_group_record(self.config['website-metadata-tablename'], new_dict.get('objectFileGroupId'), new_dict.get('storageSystem'), new_dict.get('typeOfData'))
        for item in standard_json.get('items', []):
            if datetime.now < self.time_to_break:
                self._save_curate_image_data_to_dynamo(item, export_all_files_flag, file_needed_updated)
        return file_needed_updated

    def _save_standard_json_to_dynamo_required(self, standard_json: dict) -> bool:
        """ If there is a manual request to save json to dynamo, then flag save.
            If the root record for this item doesn't already exist in dynamo, then flag save.
            If the record saved in dynamo was generated before the currently generated standard json, then flag save.
            Otherwise, flag no save needed. """
        if self.event.get('forceSaveStandardJson'):
            return True
        saved_standard_json_root_json = get_item_record(self.config.get('website-metadata-tablename', ''), standard_json.get('id'))
        if not saved_standard_json_root_json:
            return True
        if saved_standard_json_root_json.get('fileCreatedDate') < standard_json.get('fileCreatedDate'):
            return True
        return False

    def _find_first_file_in_standard_json(self, standard_json: dict) -> dict:
        """ Find the first file in standard_json """
        if standard_json.get('level') == 'file':
            return standard_json
        else:
            for item in standard_json.get('items', []):
                return_value = self._find_first_file_in_standard_json(item)
                if return_value:
                    return return_value

    def _save_file_to_dynamo_required(self, first_file_json: dict) -> bool:
        """ If the record for this file doesn't already exist in dynamo, then flag save.
            If the record saved in dynamo was generated before the modifiedDate for this record, then flag save.
            Otherwise, flag no save needed. """
        saved_file_json = get_file_record(self.config.get('website-metadata-tablename', ''), first_file_json.get('id'))
        if not saved_file_json:
            return True
        if saved_file_json.get('modifiedDate', '')[:10] < first_file_json.get('modifiedDate', '')[:10]:
            print('Need to save files because saved_file_date:', saved_file_json.get('modifiedDate', '')[:10], ' < date of first file in std json: ', first_file_json.get('modifiedDate', '')[:10])
            return True
        return False

    def _generate_new_standard_json_required(self, curate_json: dict) -> bool:
        """ If there is a manual request to save json to dynamo, then generate standard json.
            If there is no saved_standard_json, then generate standard json
            If date of existing standard json is older than the modified date in cureate, then generate standard json.
            If validate_json.py has been updated more recently than the date in standard json, then generate standard json.
            Otherwise, flag no generation needed. """
        item_id = curate_json.get('id')
        if item_id == 'qz20sq9094h' and not self.save_standard_json_locally:
            return False  # special case for Architectural Lantern Slides, which take over 2 hours to create standard json, which can only be generated locally.
        if self.event.get('forceSaveStandardJson'):
            return True
        saved_standard_json = self._get_saved_standard_json(item_id)
        if not saved_standard_json:
            return True
        date_from_curate = curate_json.get('dateSubmitted')[:10]
        date_from_saved_standard_json = saved_standard_json.get('modifiedDate')
        if not date_from_saved_standard_json or date_from_curate > date_from_saved_standard_json:
            return True
        validate_json_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'dependencies', 'pipelineutilities', 'validate_json.py')
        validate_json_modified_date = datetime.fromtimestamp(os.path.getmtime(validate_json_path))
        if validate_json_modified_date.isoformat() > date_from_saved_standard_json:
            return True
        return False
