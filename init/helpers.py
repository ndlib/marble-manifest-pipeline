# files to help the init action
from datetime import datetime, timedelta, timezone
import os


def get_file_ids_to_be_processed(files, config):
    """ looks at the file list from s3 and decides which file ids need to be processed """

    time_threshold_for_processing = determine_time_threshold_for_processing(config)
    for file in files:
        # if it is not the basedirectory which is returned in the list and
        # the time is more recent than out test threshold
        if file['Key'] != config['process-bucket-csv-basepath'] + "/" and file['LastModified'] >= time_threshold_for_processing:
            yield get_if_from_file_key(file['Key'])


def get_all_file_ids(files, config):
    for file in files:
        if file['Key'] != config['process-bucket-csv-basepath'] + "/":
            yield get_if_from_file_key(file['Key'])


def determine_time_threshold_for_processing(config):
    """ Creates the datetime object that is used to test all the files against """

    time_threshold_for_processing = datetime.utcnow() - timedelta(hours=config['hours-threshold-for-incremental-harvest'])
    # since this is utc already but there is no timezone add it in so
    # the data can be compared to the timze zone aware date in file
    return time_threshold_for_processing.replace(tzinfo=timezone.utc)


def get_if_from_file_key(key):
    """ processes the file key to get the file id """
    # remove the extension
    id = os.path.splitext(key)
    # get the basename (filename)
    return os.path.basename(id[0])


def generate_config_filename():
    return str(datetime.now()).replace(" ", "-") + ".json"
