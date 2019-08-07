# edit bucket below to be your bucket
# assume aws creds for your account
# usage python fix_csv_for_3.py
# NOTE: this will start the step functions when it uploads the mains.csv
#
from botocore.exceptions import ClientError
import boto3
import os

s3 = boto3.resource('s3')

bucket = "marble-manifest-prod-processbucket-kskqchthxshg"

replacements = [
 ['License', 'Rights'],
 ['Description', 'Summary'],
]

os.mkdir('./fix')


def _key_existing_size__head(client, bucket, key):
    """return the key's size if it exist, else None"""
    try:
        obj = client.head_object(Bucket=bucket, Key=key)
        return obj['ContentLength']
    except ClientError as exc:
        if exc.response['Error']['Code'] != '404':
            raise

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

    if _key_existing_size__head(s3.meta.client, bucket, items):
        print("skip")
        continue

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
