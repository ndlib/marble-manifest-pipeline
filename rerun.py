import boto3
import botocore
import argparse
import os.path
import pprint

# global variables
#errors = { 'validation': { 'event1': [err1,err2,err3],'event2':[err1]}, 'process': {} }
errors = { 'validation': {}, 'process': {} }

def _get_config(args):
    return {
            "process-bucket": args.bucket,
            "process-bucket-read-basepath": 'process',
            "process-bucket-write-basepath": 'finished',
            "process-bucket-last-basepath": 'lastSuccessfullRun',
            "sequence-csv": 'sequence.csv',
            "main-csv": 'main.csv'
        }

def _print_errors(etype):
    pprint.pprint(errors[etype])

def _add_error(etype, event, msg):
    if event in errors[etype]:
        errors[etype][event].append(msg)
    else:
        errors[etype][event] = [msg]

def _print_validation_errors():
    _print_errors('validation')

def _print_process_errors():
    _print_errors('process')

def _add_validation_error(msg, event='SYSTEM'):
    _add_error('validation',event, msg)

def _add_process_error(msg, event='SYSTEM'):
    _add_error('process',event, msg)

def _validation_errors_exist():
    return bool(errors['validation'])

def _process_errors_exist():
    return bool(errors['process'])

def _verify_events(file):
    return _get_events(file)

def _verify_lastrun_files(args, events):
    s3 = boto3.resource('s3')
    cfg = _get_config(args)
    for event in events:
        main_csv = cfg["process-bucket-write-basepath"] + "/" \
            + event + "/" + cfg["process-bucket-last-basepath"] + "/" \
            + cfg["main-csv"]
        seq_csv = cfg["process-bucket-write-basepath"] + "/" \
            + event + "/" + cfg["process-bucket-last-basepath"] + "/" \
            + cfg["sequence-csv"]
        for csv in [main_csv,seq_csv]:
            try:
                s3.Object(cfg["process-bucket"],csv).load()
            except botocore.exceptions.ClientError as e:
                err_msg = csv + " - " + e.response['Error']['Code'] \
                            + ": " + e.response['Error']['Message']
                _add_validation_error(err_msg,event)

def _verify_args(args):
    events = _verify_events(args.events)
    _verify_lastrun_files(args, events)

def _get_events(file):
    try:
        with open(file, 'r') as f:
            lines = f.read().split('\n')
    except IOError:
        _add_validation_error('Cannot open ' + file)
        return []
    return lines

def _setup_event(args):
    cfg = _get_config(args)
    for event in _get_events(args.events):
        try:
            # delete existing objects in the process folder
            bucket = boto3.resource('s3').Bucket(cfg['process-bucket'])
            key = cfg['process-bucket-read-basepath'] + "/" + event + "/"
            bucket.objects.filter(Prefix=key).delete()
            # copy last run objects to process folder
            objects = _get_last_run_objects(event, args)
            _copy_files_to_rerun(event, objects, args)
        except Exception as e:
            _add_process_error(e,event)
            return
            

def _get_last_run_objects(event, args):
    cfg = _get_config(args)
    last_run_path = cfg['process-bucket-write-basepath'] + '/' \
                    + event + '/' + cfg['process-bucket-last-basepath']
    return _list_s3_obj_by_dir(last_run_path,args)

def _list_s3_obj_by_dir(s3dir, args):
    s3 = boto3.client('s3')
    params = {
        'Bucket': args.bucket,
        'Prefix': s3dir,
        'StartAfter': s3dir,
    }
    attempt = 0
    keys = []
    while ('ContinuationToken' in params) or (attempt == 0):
        attempt += 1
        objects = s3.list_objects_v2(**params)
        for content in objects['Contents']:
            # skip 'folders'
            if content['Key'].endswith('/'):
                continue
            keys.append(content['Key'])
        # grab more objects to process if necessary(max 1,000/request)
        if objects['IsTruncated']:
            params['ContinuationToken'] = objects['NextContinuationToken']
        else:
            params.pop('ContinuationToken', None)
    return keys

def _copy_files_to_rerun(event, objects, args):
    cfg = _get_config(args)
    s3 = boto3.resource('s3')
    last_run_path = cfg['process-bucket-write-basepath'] + '/' \
                        + event + '/' + cfg['process-bucket-last-basepath'] + '/'
    try:
        for s3obj in objects:
            copy_src = {'Bucket':cfg['process-bucket'],'Key':s3obj}
            dest_key = cfg['process-bucket-read-basepath'] + '/' \
                            + event + '/' + s3obj[len(last_run_path):]
            s3.Object(cfg['process-bucket'],dest_key).copy_from(CopySource=copy_src)
        # s3.Object('my_bucket','my_file_old').delete()
    except Exception as e:
        _add_process_error(e,event)

def rerun(args):
    _verify_args(args)
    if _validation_errors_exist():
        _print_validation_errors()
        print("Fix the above errors and resubmit.")
        return
    else:
        print('Preparing events now...')
        _setup_event(args)
        print('Event preperation complete.')
        for event in _get_events(args.events):
            try:
                # dont attempt to launch steps on events with errors
                if event in errors['process']:
                    continue
                print('Starting to process ' + event + '...')
                params = {
                    'stateMachineArn': args.stepfn,
                    'input': "{\"id\" : \"" + event + "\"}"
                }
                boto3.client('stepfunctions').start_execution(**params)
            except Exception as e:
                _add_process_error(e, event)
        print('All events have been launched.')
        if _process_errors_exist():
            _print_process_errors()
            print("The above event errors were caught during processing.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', '-b', type=str, required=True,
        help="Bucket where event data(cvs, images) live")
    parser.add_argument('--stepfn', '-s', type=str, required=True,
        help="Step function state machine name")
    parser.add_argument('--events', '-e', type=str, required=True,
        help="Text file with an event on each row")
    args = parser.parse_args()

    rerun(args)

if __name__ == "__main__":
    main()