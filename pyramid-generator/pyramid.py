#!/usr/bin/python
import sys
import os
from pyvips import Image
import json
import boto3
import botocore

# DEBUG
# DEBUG_BUCKET = 'testlibnd-junk'
# params = {
#     'script': sys.argv[0],
#     'num args': len(sys.argv),
#     'all': str(sys.argv),
#     'sep args': sys.argv[1:]
# }

# filename = "fargate_debug.json"
# f = open(filename, "w")
# f.write(json.dumps(params))
# f.close()
# boto3.resource('s3', region_name='us-east-1').Bucket(DEBUG_BUCKET).upload_file(filename, 'fargate/' + filename)
# END DEBUG


config = json.loads(sys.argv[1])
BUCKET = config['process-bucket']
PREFIX = config['process-bucket-read-basepath'] + '/' \
    + config['id'] + '/images/'
kwargs = {'Bucket': BUCKET, 'Prefix': PREFIX}

s3client = boto3.client('s3', region_name='us-east-1')
s3resource = boto3.resource('s3', region_name='us-east-1')
image_info = {}
image_errs = {}
obj_list = s3client.list_objects_v2(**kwargs)['Contents']
for obj in obj_list:
    file = os.path.basename(obj['Key'])
    filename, file_ext = os.path.splitext(file)
    tif_file = filename + '.tif'
    # ignore some/dir/ and some/dir/default.jpg
    if obj['Key'].endswith(('/')) or filename == 'default':
        continue
    # delete some/dir/._osx_file
    if file.startswith('._'):
        try:
            print "Deleting: {}".format(obj['Key'])
            s3client.delete_object(Bucket=BUCKET, Key=obj['Key'])
        except botocore.exceptions.ClientError as e:
            print "Exception with {}".format(obj['Key'])
            image_errs[file] = str(e)
            continue
        # delete the ._osx_file and move onto the next file
        continue

    try:
        s3client.download_file(BUCKET, obj['Key'], file)
    except botocore.exceptions.ClientError as e:
        print "Exception with {}".format(obj['Key'])
        image_errs[file] = str(e)
        continue

    try:
        print 'Generating pyramid tif for: {}'.format(file)
        image = Image.new_from_file(file, access='sequential')
        image.tiffsave(tif_file, tile=True, pyramid=True, compression='deflate')
        converted_img = Image.new_from_file(tif_file, access='sequential')
        image_info[filename] = {
            u'height': converted_img.get('height'),
            u'width': converted_img.get('width')
        }
        os.remove(file)
    except Exception as e:
        image_errs[file] = str(e)
        continue

    try:
        image_dest = config['process-bucket-write-basepath'] + '/' \
            + config['id'] + '/images/' + tif_file
        print 'Putting ' + tif_file + ' to ' + image_dest
        s3resource.Bucket(BUCKET).upload_file(tif_file, image_dest)
        os.remove(tif_file)
    except botocore.exceptions.ClientError as e:
        image_errs[file] = str(e)
        continue

try:
    image_data_file = 'image_data.json'
    with open(image_data_file, 'w') as outfile:
        json.dump(image_info, outfile)
    image_data_dest = config['process-bucket-read-basepath'] + '/' \
        + config['id'] + '/' + image_data_file
    s3resource.Bucket(BUCKET).upload_file(image_data_file, image_data_dest)
except botocore.exceptions.ClientError as e:
    image_errs[file] = str(e)

try:
    if image_errs:
        err_file = "image_err.json"
        s3err_dest = config['process-bucket-read-basepath'] + '/' \
            + config['id'] + '/' + err_file
        with open(err_file, 'w') as outfile:
            json.dump(image_errs, outfile)
        s3resource.Bucket(BUCKET).upload_file(err_file, s3err_dest)
except botocore.exceptions.ClientError as e:
    print "Couldnt write error file to bucket!"
