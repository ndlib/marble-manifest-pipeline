"""
This module will generate a new AppSync API key and will store the value of the key
    in a SecureString in Parameter Store named <SMM_KEY_BASE>/graphql-api-key
"""

import os
import boto3
import datetime
import botocore
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

if 'SENTRY_DSN' in os.environ:
    sentry_sdk.init(dsn=os.environ['SENTRY_DSN'], integrations=[AwsLambdaIntegration()])


def run(event, _context):
    """ run the process to retrieve and process web kiosk metadata """
    ssm_key_base = os.environ.get('SSM_KEY_BASE')
    graphql_api_id_key_path = os.path.join(ssm_key_base, 'graphql-api-id')  # need to add this to ssm via marble-blueprints
    graphql_api_id = _get_parameter(graphql_api_id_key_path)
    # graphql_api_id = 'e7mqujivhrar3oc7tbrf5gy5lm'  # need this in ssm
    # graphql_api_id = 'moazpuqvgvfy3dfy7maqjfihpq'  # need this in ssm
    # graphql_api_key_key_path = os.path.join(ssm_key_base, 'graphql-api-key')
    # rotate_graphql_api_keys(graphql_api_id, graphql_api_key_key_path)
    _delete_expired_api_keys(graphql_api_id)
    return event


def rotate_graphql_api_keys(graphql_api_id: str, graphql_api_key_key_path: str) -> str:
    """ Given a graphql api id, generate a new api_key and store the value in
        the parameter store location specified by graphql_agraphql_api_key_key_pathpi_key_path.
        Return the new api_key """
    api_key = _generate_new_api_key(graphql_api_id, _get_expire_time())
    _save_secure_parameter(graphql_api_key_key_path, api_key)

    graphql_api_key_from_ssm = _get_parameter(graphql_api_key_key_path)
    if graphql_api_key_from_ssm != api_key:
        print("parameter store key does not matche api_key")
    return api_key


def _get_parameter(name: str) -> str:
    try:
        response = boto3.client('ssm').get_parameter(Name=name, WithDecryption=True)
        value = response.get('Parameter').get('Value')
        return value
    except botocore.exceptions.ClientError:
        return None


def _generate_new_api_key(graphql_api_id: str, new_expire_time: int) -> str:
    response = boto3.client('appsync').create_api_key(apiId=graphql_api_id, description='auto maintained api key', expires=new_expire_time)
    key_id = response.get('apiKey').get('id')
    return key_id


def _delete_expired_api_keys(graphql_api_id: str):
    response = boto3.client('appsync').list_api_keys(apiId=graphql_api_id)
    for api_key in response.get('apiKeys'):
        if api_key.get('expires') < datetime.datetime.now().timestamp():
            boto3.client('appsync').delete_api_key(apiId=graphql_api_id, id=api_key.get('id'))


def _get_expire_time(days: int = 3) -> int:
    if days > 364:  # AppSync requires a key to expire less than 365 days in the future
        days = 364
    new_expire_time = (datetime.datetime.now() + datetime.timedelta(days=days)).timestamp()
    return int(new_expire_time)


def _save_secure_parameter(name: str, key_id: str) -> bool:
    try:
        boto3.client('ssm').put_parameter(Name=name, Description='api key for graphql-api-url', Value=key_id, Type='SecureString', Overwrite=True)
        return True
    except botocore.exceptions.ClientError:
        return False


# setup:
# export SSM_KEY_BASE='/all/stacks/steve-maintain-metadata'
# aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
# python -c 'from handler import *; test()'
def test():
    """ test exection """
    event = {}
    event = run(event, {})
    print("event = ", event)
