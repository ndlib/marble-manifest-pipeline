import boto3
import os
import sys
from urllib.parse import unquote
sys.path.append(os.path.dirname(os.path.realpath(__file__)))


def run(event, context):
    """
    Executes state machine when main.csv 
    is dropped in the process bucket.

    Args:
        event (dict): AWS S3 event data
        context (dict): AWS S3 context data
    """
    try:
        statemachine_arn = os.environ['STATE_MACHINE']
        eventid = unquote(event['Records'][0]['s3']['object']['key'].split("/")[1])
        boto3.client('stepfunctions').start_execution(
            stateMachineArn=statemachine_arn,
            input="{\"id\" : \"" + eventid + "\"}"
        )
    except Exception as e:
        print(e)
    return event
