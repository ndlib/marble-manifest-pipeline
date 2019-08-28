import json
import boto3

def lambda_handler(event, context):
    from mapcollection import mapManifestCollection
    s3 = boto3.client('s3')
    bucket_name = event['config']['manifest-server-bucket']
    file_key = event['config']['event-file']
    rfile = s3.get_object(Bucket=bucket_name, Key=file_key)
    fileFolder = file_key.split('.', 1)[0]
    alsoLocation = bucket_name+'.s3.amazonaws.com/finished/'+fileFolder
    readfile = json.load(rfile['Body'])
    type = readfile['type']
    if type == 'Collection':
        mapManifestCollection(readfile, 'CreativeWorkSeries', fileFolder, bucket_name)
    elif type == 'Manifest':
        mapManifestCollection(readfile, 'CreativeWork', fileFolder, bucket_name)
    else:
        print ("Unknown Manifest")
        return {
            'statusCode': 415
        }
    readfile.update({"seeAlso": alsoLocation})
    readfileUpdate = json.dumps(readfile)
    wfile = s3.put_object(Body=readfileUpdate, Bucket=bucket_name, Key=file_key)
    return {
        'statusCode': 200
    }
