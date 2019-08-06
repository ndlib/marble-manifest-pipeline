# edit bucket below to be your bucket
# assume aws creds for your account
# usage python fix_csv_for_3.py
# NOTE: this will start the step functions when it uploads the mains.csv
#
import boto3
import os

s3 = boto3.resource('s3')

bucket = "manifest-pipeline-v3-processbucket-kmnll9wpj2h3"

replacements = [
 ['License', 'Rights'],
 ['Description', 'Summary'],
]

os.mkdir('./fix')

local_main = os.path.join('fix', 'main.csv')
local_sequence = os.path.join('fix', 'sequence.csv')
local_item = os.path.join('fix', 'items.csv')

result = s3.meta.client.list_objects(Bucket=bucket, Prefix="process/", Delimiter='/')
for o in result.get('CommonPrefixes'):
    id = o.get('Prefix').replace("process/", "").replace("/", "")
    print(id)

    main = os.path.join('process', id, 'main.csv')
    sequence = os.path.join('process', id, 'sequence.csv')
    items = os.path.join('process', id, 'items.csv')

    s3.meta.client.download_file(bucket, main, local_main)
    s3.meta.client.download_file(bucket, sequence, local_sequence)

    f = open(local_main, "r")
    main_text = f.read()
    for replacement in replacements:
        main_text = main_text.replace(replacement[0], replacement[1])
    f.close()

    f = open(local_main, "w")
    f.write(main_text)
    f.close()

    os.rename(local_sequence, local_item)

    s3.meta.client.upload_file(local_item, bucket, items)
    s3.meta.client.upload_file(local_main, bucket, main)    

# s3.Object(bucket, sequence).delete()
os.rmdir('./fix')
