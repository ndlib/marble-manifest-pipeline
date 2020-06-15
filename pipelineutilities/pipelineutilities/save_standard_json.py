import os
from s3_helpers import write_s3_json, s3_file_exists
from record_files_needing_processed import FilesNeedingProcessed
from validate_json import validate_standard_json


def save_standard_json(config: dict, standard_json: dict, export_all_files_flag: bool = False) -> bool:
    """ First, validate the standard_json.  If this is the first time this standard_json is being saved,
        we will need to make sure all images and related files get processed.
        We next call a process to record files needing processed.
        We then save the standard_json. """
    success_flag = False
    if validate_standard_json(standard_json):
        if "id" in standard_json:
            key_name = os.path.join(config['process-bucket-data-basepath'], standard_json["id"] + '.json')
            standard_json_already_exists_flag = s3_file_exists(config['process-bucket'], key_name)
            if not export_all_files_flag:
                export_all_files_flag = (not standard_json_already_exists_flag)
            files_needing_processed_class = FilesNeedingProcessed(config)
            if files_needing_processed_class.record_files_needing_processed(standard_json, export_all_files_flag):
                _save_json_to_s3(config['process-bucket'], key_name, standard_json)
                # print("saved standard_json to ", config['process-bucket'])
                success_flag = True
    return success_flag


def _save_json_to_s3(s3_bucket_name: str, json_file_name: str, json_record: dict) -> bool:
    try:
        write_s3_json(s3_bucket_name, json_file_name, json_record)
        results = True
    except Exception:
        results = False
    return results
