import os
import sys
import json
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

from finalizeStep import finalizeStep


def run(event, context):
    step = finalizeStep(event.get("id"), event)
    step.error = event.get("unexpected", "")
    if not step.error:
        step.manifest_metadata = step.read_event_data()
    step.run()

    return event


# python -c 'from handler import *; test()'
def test():
    with open("../example/example-input.json", 'r') as input_source:
        data = json.load(input_source)
    input_source.close()

    data = {
      "id": "example",
      "data": data
    }
    # print(run(data, {}))
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
                  "Value": "eni-092fc5ebb46547d69"
                },
                {
                  "Name": "macAddress",
                  "Value": "0a:04:3d:64:b1:48"
                },
                {
                  "Name": "privateIPv4Address",
                  "Value": "10.0.3.118"
                }
              ],
              "Id": "6d4d6a08-50c1-4d1c-b187-601d5c192d3f",
              "Status": "DELETED",
              "Type": "eni"
            }
          ],
          "ClusterArn": "arn:aws:ecs:us-east-1:333680067100:cluster/marble-app-infrastructure-Cluster",
          "Connectivity": "CONNECTED",
          "ConnectivityAt": 1568727788376,
          "Containers": [
            {
              "ContainerArn": "arn:aws:ecs:us-east-1:333680067100:container/579e2acd-b9e0-45ae-8754-d8626349b07c",
              "Cpu": "1024",
              "ExitCode": 0,
              "GpuIds": [],
              "LastStatus": "STOPPED",
              "Memory": "2048",
              "Name": "manifest-pipeline-v3-Manifest",
              "NetworkBindings": [],
              "NetworkInterfaces": [
                {
                  "AttachmentId": "6d4d6a08-50c1-4d1c-b187-601d5c192d3f",
                  "PrivateIpv4Address": "10.0.3.118"
                }
              ],
              "TaskArn": "arn:aws:ecs:us-east-1:333680067100:task/marble-app-infrastructure-Cluster/5c704c7e4f1348f3969072b112bdaaf0"
            }
          ],
          "Cpu": "1024",
          "CreatedAt": 1568727784593,
          "DesiredStatus": "STOPPED",
          "ExecutionStoppedAt": 1568727876000,
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
          "PullStartedAt": 1568727801294,
          "PullStoppedAt": 1568727822294,
          "StartedAt": 1568727825294,
          "StartedBy": "AWS Step Functions",
          "StopCode": "EssentialContainerExited",
          "StoppedAt": 1568727899134,
          "StoppedReason": "Essential container in task exited",
          "StoppingAt": 1568727886597,
          "Tags": [],
          "TaskArn": "arn:aws:ecs:us-east-1:333680067100:task/marble-app-infrastructure-Cluster/5c704c7e4f1348f3969072b112bdaaf0",
          "TaskDefinitionArn": "arn:aws:ecs:us-east-1:333680067100:task-definition/manifest-pipeline-v3-Manifest:2",
          "Version": 6
        }
      },
      "index-marble": True,
      "notify-on-finished": "rfox2@nd.edu"
    }
    run(event, {})
