import os
import json
import boto3
from iiifManifest import iiifManifest


def run(event, context):
    id = event.get('id')
    s3_bucket = event['process-bucket']
    s3_event_path = os.path.join(event['process-bucket-read-basepath'], id, event["event-file"])
    s3_manifest_path = os.path.join(event['process-bucket-write-basepath'], id, 'manifest/index.json')

    # for testing see test() below.
    # This allows this to be run locally without having a file in s3
    if event.get('manifestData'):
        manifestData = event.get('manifestData')
    else:
        manifestData = json.loads(read_s3_file_content(s3_bucket, s3_event_path))

    # get manifest object
    manifest = iiifManifest(event, manifestData)
    # write to s3
    write_s3_json(s3_bucket, s3_manifest_path, manifest.manifest())

    return event


def read_s3_file_content(s3_bucket, s3_path):
    content_object = boto3.resource('s3').Object(s3_bucket, s3_path)
    return content_object.get()['Body'].read().decode('utf-8')


def write_s3_json(s3_bucket, s3_path, json_hash):
    s3 = boto3.resource('s3')
    s3.Object(s3_bucket, s3_path).put(Body=json.dumps(json_hash), ContentType='text/json')


# python -c 'from handler import *; test()'
def test():
    with open("../example/item-one-image/event.json", 'r') as input_source:
        data = json.load(input_source)
    input_source.close()

    with open("../example/item-one-image/config.json", 'r') as input_source:
        config = json.load(input_source)
    input_source.close()

    # print(iiifManifest(config, data).manifest())
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
                  "Value": "eni-0350633bf24afe93f"
                },
                {
                  "Name": "macAddress",
                  "Value": "0a:fe:2a:46:c3:00"
                },
                {
                  "Name": "privateIPv4Address",
                  "Value": "10.0.3.164"
                }
              ],
              "Id": "bf5677f5-a2eb-45ff-b33d-d59f0bacff7d",
              "Status": "DELETED",
              "Type": "eni"
            }
          ],
          "ClusterArn": "arn:aws:ecs:us-east-1:333680067100:cluster/marble-app-infrastructure-Cluster",
          "Connectivity": "CONNECTED",
          "ConnectivityAt": 1568726143618,
          "Containers": [
            {
              "ContainerArn": "arn:aws:ecs:us-east-1:333680067100:container/3971ccc6-c798-4e06-9ee7-196a484944a5",
              "Cpu": "1024",
              "ExitCode": 0,
              "GpuIds": [],
              "LastStatus": "STOPPED",
              "Memory": "2048",
              "Name": "manifest-pipeline-v3-Manifest",
              "NetworkBindings": [],
              "NetworkInterfaces": [
                {
                  "AttachmentId": "bf5677f5-a2eb-45ff-b33d-d59f0bacff7d",
                  "PrivateIpv4Address": "10.0.3.164"
                }
              ],
              "TaskArn": "arn:aws:ecs:us-east-1:333680067100:task/marble-app-infrastructure-Cluster/87c201c181e44ae4b8b704c712b0fbc3"
            }
          ],
          "Cpu": "1024",
          "CreatedAt": 1568726139512,
          "DesiredStatus": "STOPPED",
          "ExecutionStoppedAt": 1568726249000,
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
          "PullStartedAt": 1568726156687,
          "PullStoppedAt": 1568726175687,
          "StartedAt": 1568726180687,
          "StartedBy": "AWS Step Functions",
          "StopCode": "EssentialContainerExited",
          "StoppedAt": 1568726272918,
          "StoppedReason": "Essential container in task exited",
          "StoppingAt": 1568726259959,
          "Tags": [],
          "TaskArn": "arn:aws:ecs:us-east-1:333680067100:task/marble-app-infrastructure-Cluster/87c201c181e44ae4b8b704c712b0fbc3",
          "TaskDefinitionArn": "arn:aws:ecs:us-east-1:333680067100:task-definition/manifest-pipeline-v3-Manifest:2",
          "Version": 5
        }
      },
      "index-marble": True,
      "notify-on-finished": "rfox2@nd.edu"
    }
    run(event, {})
