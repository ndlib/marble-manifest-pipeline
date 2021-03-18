
import boto3
import botocore
import os


def run(event, _context):
    """ save string API Key as SecureString """
    graphql_api_key_key_path = os.environ.get('GRAPHQL_API_KEY_KEY_PATH')
    print("graphql_api_key_key_path =", graphql_api_key_key_path)
    graphql_api_key = _get_parameter(graphql_api_key_key_path)
    if graphql_api_key:
        _save_secure_parameter(graphql_api_key_key_path, graphql_api_key)
        print("graphql_api_key saved as ssm SecureString")
    return event


def _get_parameter(name: str) -> str:
    try:
        response = boto3.client('ssm').get_parameter(Name=name, WithDecryption=True)
        value = response.get('Parameter').get('Value')
        return value
    except botocore.exceptions.ClientError:
        return None


def _save_secure_parameter(name: str, key_id: str) -> bool:
    try:
        boto3.client('ssm').put_parameter(Name=name, Description='api key for graphql-api-url', Value=key_id, Type='SecureString', Overwrite=True)
        return True
    except botocore.exceptions.ClientError:
        return False


# setup:
# export GRAPHQL_API_KEY_KEY_PATH='/all/stacks/steve-maintain-metadata/graphql-api-key'
# aws-vault exec testlibnd-superAdmin
# python -c 'from save_api_key_as_secure_string import *; test()'
def test():
    """ test exection """
    event = {}
    event = run(event, {})
    print("event = ", event)
