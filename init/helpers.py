from datetime import datetime, timedelta, timezone
import os


def get_file_ids_to_be_processed(files, config):
    time_threshold_for_processing = determine_time_threshold_for_processing(config)
    for file in files:
        # if it is not the basedirectory which is returned in the list and
        # the time is more recent than out test threshold
        if file['Key'] != config['process-bucket-csv-basepath'] + "/" and file['LastModified'] >= time_threshold_for_processing:
            yield get_if_from_file_key(file['Key'])


def determine_time_threshold_for_processing(config):
    time_threshold_for_processing = datetime.utcnow() - timedelta(hours=config['hours-threshold-for-incremental-harvest'])
    # since this is utc already but there is no timezone add it in so
    # the data can be compared to the timze zone aware date in file
    return time_threshold_for_processing.replace(tzinfo=timezone.utc)


def get_if_from_file_key(key):
    # remove the extension
    id = os.path.splitext(key)
    # get the basename (filename)
    return os.path.basename(id[0])
