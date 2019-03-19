#!/usr/bin/python
import sys
import os
from pyvips import Image
import boto3
import botocore
import datetime

print("This is the name of the script: " + sys.argv[0])
print("Number of arguments: ", len(sys.argv))
print("The arguments are: " + str(sys.argv))

if len(sys.argv) < 2:
    sys.exit("Required param with s3 key to process")

# https://s3-us-west-2.amazonaws.com/mellon-data-broker-dev-publicbucket-856nkug1cnvl/2018_049_004.tif
IMAGE_FROM_BUCKET_NAME = os.environ["IMAGE_FROM_BUCKET_NAME"]
IMAGE_TO_BUCKET_NAME = os.environ["IMAGE_TO_BUCKET_NAME"]

KEY = sys.argv[1]  # s3 object without extension

s3 = boto3.resource('s3', region_name='us-west-2')

print(datetime.datetime.now())
try:
    print('Retrieving file: ' + KEY + '.jpg')
    s3.Bucket(IMAGE_FROM_BUCKET_NAME).download_file(KEY + '.jpg', KEY + '.jpg')
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        sys.exit("The object does not exist.")
    else:
        raise
print('File retrieved')
print(datetime.datetime.now())

files = [KEY]
vips_error = {}
print(datetime.datetime.now())
for file in files:
    try:
        print('Generating pyramid tif for: ' + KEY + '.jpg')
        image = Image.new_from_file(KEY + '.jpg', access='sequential')
        image.tiffsave(KEY + '.tif', tile=True, pyramid=True, compression='deflate')
    except Exception as e:
        vips_error[file] = {'msg': str(e)}
        print(vips_error[file])
print(datetime.datetime.now())

print(datetime.datetime.now())
try:
    print('Putting file: ' + KEY + '.tif')
    s3.Bucket(IMAGE_TO_BUCKET_NAME).upload_file(KEY + '.tif', 'test/' + KEY + '.tif')
except botocore.exceptions.ClientError as e:
    sys.exit(e)

print('File pushed')
print(datetime.datetime.now())
