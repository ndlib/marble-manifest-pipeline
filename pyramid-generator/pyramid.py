#!/usr/bin/python
import sys
import os
print os.environ["PATH"]
from pyvips import Image
import json
import boto3
import botocore
import datetime

#https://s3-us-west-2.amazonaws.com/mellon-data-broker-dev-publicbucket-856nkug1cnvl/2018_049_004.tif
BUCKET_NAME = 'mellon-data-broker-publicbucket-wtci53auglzt'
KEY = '2018_049_004' # s3 object without extension

s3 = boto3.resource('s3', region_name='us-west-2')

print(datetime.datetime.now())
try:
    print 'Retrieving file: ' + KEY + '.jpg'
    s3.Bucket(BUCKET_NAME).download_file(KEY + '.jpg', KEY + '.jpg')
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
    else:
        raise
print 'File retrieved'
print(datetime.datetime.now())

files = [KEY]
vips_error = {}
print(datetime.datetime.now())
for file in files:
  try:
    print 'Generating pyramid tif for: ' + KEY + '.jpg'
    image = Image.new_from_file(KEY + '.jpg', access='sequential')
    image.tiffsave(KEY + '.tif', tile=True, pyramid=True, compression='deflate')
  except Exception as e:
    vips_error[file] = {'msg': str(e)}
    print vips_error[file]
print(datetime.datetime.now())


print(datetime.datetime.now())
try:
   print 'Putting file: ' + KEY + '.tif'
   s3.Bucket(BUCKET_NAME).upload_file(KEY + '_DEFLATE.tif','redTest/' + KEY + '.tif')
except botocore.exceptions.ClientError as e:
   print e
print 'File pushed'
print(datetime.datetime.now())
