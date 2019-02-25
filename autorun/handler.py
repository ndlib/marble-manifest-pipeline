import boto3
import os, sys, json
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
        s3info = event['Records'][0]['s3']
        stackname = s3info['bucket']['name'].split("-processbucket")[0]
        eventid = s3info['object']['key'].split("/")[1]
        potential_machine = stackname + '-StateMachine'
        statemachine_arn = _retrieve_statemachine(potential_machine)
        if statemachine_arn:
            boto3.client('stepfunctions').start_execution(
                stateMachineArn=statemachine_arn,
                input="{\"id\" : \"" + eventid + "\"}"
            )
        else:
            print("State Machine could not be located: " + potential_machine)
    except Exception as e:
        print(e)
    return event

def _retrieve_statemachine(stackname):
    """
    Retrieves state machine arn from stackname

    Args:
        stackname (str): name of the aws stack
    """
    try:
        client = boto3.client('stepfunctions')
        response = client.list_state_machines()
        statemachine_arn = None
        for machine in response['stateMachines']:
            if stackname in machine['name']:
                statemachine_arn = machine['stateMachineArn']
    except Exception as e:
        statemachine_arn = None
        print(e)
    return statemachine_arn
