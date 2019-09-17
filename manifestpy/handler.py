import os
import json
import boto3
from mapmain import mapMainManifest


def run(event, context):
    id = event.get('id')

    s3_bucket = event['process-bucket']
    s3_event_json_path = os.path.join(event['process-bucket-read-basepath'], id, event["event-file"])
    s3_schema_json_path = os.path.join(event['process-bucket-write-basepath'], id, 'schema/index.json')

    event_json = json.loads(read_s3_file_content(s3_bucket, s3_event_json_path))

    schema_dict = get_schema_dict(event_json)
    event_json = add_schema_to_event_json(event_json, event)

    # write the
    write_s3_json(s3_bucket, s3_event_json_path, event_json)
    write_s3_json(s3_bucket, s3_schema_json_path, schema_dict)

    return event


def add_schema_to_event_json(event_json, event):
    schema_url = os.path.join(event.get('manifest-server-base-url'), event.get('id'), 'schema')

    see_also = {
        "id": schema_url,
        "format": "application/ld+json",
        "profile": "http://iiif.io/community/profiles/discovery/schema"
    }

    event_json['seeAlso'] = event_json.get('seeAlso', [])
    event_json['seeAlso'].append(see_also)

    return event_json


def get_schema_dict(event_json):
    type = event_json.get('type', 'manifest').lower()

    if type == 'collection':
        return mapMainManifest(event_json, 'CreativeWorkSeries')
    elif type == 'manifest':
        return mapMainManifest(event_json, 'CreativeWork')

    raise "type, {}, not supported.".format(type)


def read_s3_file_content(s3_bucket, s3_path):
    content_object = boto3.resource('s3').Object(s3_bucket, s3_path)
    return content_object.get()['Body'].read().decode('utf-8')


def write_s3_json(s3_bucket, s3Path, manifest):
    s3 = boto3.resource('s3')
    s3.Object(s3_bucket, s3Path).put(Body=json.dumps(manifest), ContentType='text/json')


def test():
    with open("../example/item-one-image/config.json", 'r') as input_source:
        event = json.load(input_source)
    input_source.close()

    with open("../example/item-one-image/event.json", 'r') as input_source:
        event_json = json.load(input_source)
    input_source.close()

    schema_dict = get_schema_dict(event_json)
    event_json = add_schema_to_event_json(event_json, event)

    print("SCHEMA:")
    print(schema_dict)
    print("EVENT_JSON:")
    print(event_json)

    event = {
      "id": "epistemological-letters-issue-1",
      "process-bucket-read-basepath": "process",
      "process-bucket-write-basepath": "finished",
      "process-bucket-index-basepath": "index",
      "image-server-bucket-basepath": "",
      "manifest-server-bucket-basepath": "",
      "sequence-csv": "sequence.csv",
      "items-csv": "items.csv",
      "main-csv": "main.csv",
      "canvas-default-height": 2000,
      "canvas-default-width": 2000,
      "image-data-file": "image_data.json",
      "event-file": "event.json",
      "image-server-base-url": "https://image-iiif.libraries.nd.edu:8182/iiif/2",
      "image-server-bucket": "marble-data-broker-publicbucket-kebe5zkimvyg",
      "manifest-server-base-url": "https://manifest-pipeline-v3.libraries.nd.edu",
      "manifest-server-bucket": "manifest-pipeline-v3-manifestbucket-1dxmq1ws0o3ah",
      "process-bucket": "manifest-pipeline-v3-processbucket-kmnll9wpj2h3",
      "noreply-email-addr": "noreply@nd.edu",
      "troubleshoot-email-addr": "rdought1@nd.edu",
      "event_id": "epistemological-letters-issue-1",
      "ecs-args": [
        "{\"id\": \"epistemological-letters-issue-1\", \"process-bucket-read-basepath\": \"process\", \"process-bucket-write-basepath\": \"finished\", \"process-bucket-index-basepath\": \"index\", \"image-server-bucket-basepath\": \"\", \"manifest-server-bucket-basepath\": \"\", \"sequence-csv\": \"sequence.csv\", \"items-csv\": \"items.csv\", \"main-csv\": \"main.csv\", \"canvas-default-height\": 2000, \"canvas-default-width\": 2000, \"image-data-file\": \"image_data.json\", \"event-file\": \"event.json\", \"image-server-base-url\": \"https://image-iiif.libraries.nd.edu:8182/iiif/2\", \"image-server-bucket\": \"marble-data-broker-publicbucket-kebe5zkimvyg\", \"manifest-server-base-url\": \"https://manifest-pipeline-v3.libraries.nd.edu\", \"manifest-server-bucket\": \"manifest-pipeline-v3-manifestbucket-1dxmq1ws0o3ah\", \"process-bucket\": \"manifest-pipeline-v3-processbucket-kmnll9wpj2h3\", \"noreply-email-addr\": \"noreply@nd.edu\", \"troubleshoot-email-addr\": \"rdought1@nd.edu\", \"event_id\": \"epistemological-letters-issue-1\"}"
      ],
      "metadata-source-type": "csv",
      "ecs": {
        "ecsresults": {
          "Attachments": [
            {
              "Details": [
                {
                  "Name": "subnetId",
                  "Value": "subnet-002db0755f2351cb7"
                },
                {
                  "Name": "networkInterfaceId",
                  "Value": "eni-0973a04778ef78d10"
                },
                {
                  "Name": "macAddress",
                  "Value": "0a:3f:62:ba:16:56"
                },
                {
                  "Name": "privateIPv4Address",
                  "Value": "10.0.3.180"
                }
              ],
              "Id": "5768aaa8-8b62-4cdb-9a37-4ff895982195",
              "Status": "DELETED",
              "Type": "eni"
            }
          ],
          "ClusterArn": "arn:aws:ecs:us-east-1:333680067100:cluster/marble-app-infrastructure-Cluster",
          "Connectivity": "CONNECTED",
          "ConnectivityAt": 1568725031717,
          "Containers": [
            {
              "ContainerArn": "arn:aws:ecs:us-east-1:333680067100:container/079a7932-75a7-48e9-a9aa-ff45a09fa796",
              "Cpu": "1024",
              "ExitCode": 0,
              "GpuIds": [],
              "LastStatus": "STOPPED",
              "Memory": "2048",
              "Name": "manifest-pipeline-v3-Manifest",
              "NetworkBindings": [],
              "NetworkInterfaces": [
                {
                  "AttachmentId": "5768aaa8-8b62-4cdb-9a37-4ff895982195",
                  "PrivateIpv4Address": "10.0.3.180"
                }
              ],
              "TaskArn": "arn:aws:ecs:us-east-1:333680067100:task/marble-app-infrastructure-Cluster/e148e6d9ed4a41929114dc9869965d99"
            }
          ],
          "Cpu": "1024",
          "CreatedAt": 1568725027260,
          "DesiredStatus": "STOPPED",
          "ExecutionStoppedAt": 1568725122000,
          "Group": "family:manifest-pipeline-v3-Manifest",
          "LastStatus": "STOPPED",
          "LaunchType": "FARGATE",
          "Memory": "2048",
          "Overrides": {
            "ContainerOverrides": [
              {
                "Command": [
                  "{\"id\": \"epistemological-letters-issue-1\", \"process-bucket-read-basepath\": \"process\", \"process-bucket-write-basepath\": \"finished\", \"process-bucket-index-basepath\": \"index\", \"image-server-bucket-basepath\": \"\", \"manifest-server-bucket-basepath\": \"\", \"sequence-csv\": \"sequence.csv\", \"items-csv\": \"items.csv\", \"main-csv\": \"main.csv\", \"canvas-default-height\": 2000, \"canvas-default-width\": 2000, \"image-data-file\": \"image_data.json\", \"event-file\": \"event.json\", \"image-server-base-url\": \"https://image-iiif.libraries.nd.edu:8182/iiif/2\", \"image-server-bucket\": \"marble-data-broker-publicbucket-kebe5zkimvyg\", \"manifest-server-base-url\": \"https://manifest-pipeline-v3.libraries.nd.edu\", \"manifest-server-bucket\": \"manifest-pipeline-v3-manifestbucket-1dxmq1ws0o3ah\", \"process-bucket\": \"manifest-pipeline-v3-processbucket-kmnll9wpj2h3\", \"noreply-email-addr\": \"noreply@nd.edu\", \"troubleshoot-email-addr\": \"rdought1@nd.edu\", \"event_id\": \"epistemological-letters-issue-1\"}"
                ],
                "Environment": [],
                "Name": "manifest-pipeline-v3-Manifest",
                "ResourceRequirements": []
              }
            ]
          },
          "PlatformVersion": "1.3.0",
          "PullStartedAt": 1568725043448,
          "PullStoppedAt": 1568725064448,
          "StartedAt": 1568725069448,
          "StartedBy": "AWS Step Functions",
          "StopCode": "EssentialContainerExited",
          "StoppedAt": 1568725145181,
          "StoppedReason": "Essential container in task exited",
          "StoppingAt": 1568725132545,
          "Tags": [],
          "TaskArn": "arn:aws:ecs:us-east-1:333680067100:task/marble-app-infrastructure-Cluster/e148e6d9ed4a41929114dc9869965d99",
          "TaskDefinitionArn": "arn:aws:ecs:us-east-1:333680067100:task-definition/manifest-pipeline-v3-Manifest:2",
          "Version": 5
        }
      },
      "index-marble": True,
      "notify-on-finished": "rfox2@nd.edu",
      "unexpected": {
        "Error": "AttributeError",
        "Cause": "{\"errorMessage\": \"'NoneType' object has no attribute 'get'\", \"errorType\": \"AttributeError\", \"stackTrace\": [\"  File \\\"/var/task/handler.py\\\", line 16, in run\\n    schema_dict = get_schema_dict(event_json)\\n\", \"  File \\\"/var/task/handler.py\\\", line 41, in get_schema_dict\\n    type = event_json.get('type', 'manifest').lower()\\n\"]}"
      }
    }
    run(event, {})
