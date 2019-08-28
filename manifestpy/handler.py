import json
import boto3


def run(event, context):
    from mapcollection import mapManifestCollection
    s3 = boto3.client('s3')
    bucket_name = event['config']['manifest-server-bucket']
    file_key = event['config']['event-file']
    rfile = s3.get_object(Bucket=bucket_name, Key=file_key)
    file_folder = file_key.split('.', 1)[0]
    also_location = bucket_name+'.s3.amazonaws.com/finished/'+file_folder
    readfile = json.load(rfile['Body'])
    type = readfile['type']
    if type == 'Collection':
        mapManifestCollection(readfile, 'CreativeWorkSeries', file_folder, bucket_name)
    elif type == 'Manifest':
        mapManifestCollection(readfile, 'CreativeWork', file_folder, bucket_name)
    else:
        print("Unknown Manifest")
        return {
            'statusCode': 415
        }
    readfile.update({"seeAlso": also_location})
    readfile_update = json.dumps(readfile)
    s3.put_object(Body=readfile_update, Bucket=bucket_name, Key=file_key)
    return {
        'statusCode': 200
    }


def test():
    with open("./test_data.json", 'r') as input_source:
        data = json.load(input_source)
    input_source.close()
    data = {
      "id": "example",
      "config": data["config"],
      "manifestData": data
    }
    print(run(data, {}))
