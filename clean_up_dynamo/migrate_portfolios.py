""" handler """

import _set_path  # noqa
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import sentry_sdk   # noqa: E402
from dynamo_helpers import format_key_value, get_iso_date_as_string
import datetime
import json


def query_dynamo_records(table_name: str, **kwargs) -> dict:
    """ very generic dynamo query """
    response = {}
    table = boto3.resource('dynamodb').Table(table_name)
    response = table.query(**kwargs)
    return response
    try:
        table = boto3.resource('dynamodb').Table(table_name)
        response = table.query(**kwargs)
    except ClientError as ce:
        sentry_sdk.capture_exception(ce)
    return response

# setup:
# aws-vault exec testlibnd-superAdmin
# python -c 'from migrate_portfolios import *; test()'


def query_first_result_from_table(table_name: str, pk_field_name: str, pk_value: str) -> dict:
    kwargs = {}
    kwargs['KeyConditionExpression'] = Key(pk_field_name).eq(pk_value)

    results = query_dynamo_records(table_name, **kwargs)
    for record in results.get('Items', []):
        return record


def read_existing_collection(collection_uuid: str) -> dict:
    collections_table_name = 'marbleb-prod-user-content-CollectionsTable0BB1CCFE-1WVFNCS19OQI9'  # PK = uuid
    return(query_first_result_from_table(collections_table_name, 'uuid', collection_uuid))


def read_existing_user(user_uuid: str) -> dict:
    users_table_name = 'marbleb-prod-user-content-UsersTable9725E9C8-YKMZ2TWYQ7YW'  # PK = uuid
    return(query_first_result_from_table(users_table_name, 'uuid', user_uuid))


def read_all_existing_items_for_a_collection(collection_uuid: str) -> list:
    items_table_name = 'marbleb-prod-user-content-ItemsTable5AAC2C46-AQSOZM9WNXR4'  # PK = uuid, index = collectionId, key is collectionId
    items_results = []
    kwargs = {}
    kwargs['IndexName'] = 'collectionId'
    kwargs['KeyConditionExpression'] = Key('collectionId').eq(collection_uuid)
    while True:
        results = query_dynamo_records(items_table_name, **kwargs)
        for record in results.get('Items', []):
            items_results.append(record)
        if results.get('LastEvaluatedKey'):
            kwargs['ExclusiveStartKey'] = results.get('LastEvaluatedKey')
        else:
            break
    return items_results


def make_new_user_json(existing_user_json: dict) -> dict:
    json_record = {}
    json_record['bio'] = ''
    if existing_user_json.get('bio') != '{{NULL}}':
        json_record['bio'] = existing_user_json.get('bio')
    json_record['dateAddedToDynamo'] = datetime.datetime.fromtimestamp(existing_user_json.get('created') / 1000).isoformat()
    json_record['dateModifiedInDynamo'] = datetime.datetime.fromtimestamp(existing_user_json.get('updated') / 1000).isoformat()
    json_record['department'] = ''
    json_record['email'] = existing_user_json.get('email')
    json_record['fullName'] = existing_user_json.get('fullName')
    json_record['PK'] = 'PORTFOLIO'
    json_record['portfolioUserId'] = existing_user_json.get('userName')
    json_record['primaryAffiliation'] = 'staff'
    if existing_user_json.get('userName') in ['mnarlock']:
        json_record['primaryAffiliation'] = 'faculty'
    if existing_user_json.get('userName') in ['nmurphy3']:
        json_record['primaryAffiliation'] = 'student'
    json_record['SK'] = 'USER#' + format_key_value(existing_user_json.get('userName'))
    json_record['TYPE'] = 'PortfolioUser'
    json_record['migratedDate'] = get_iso_date_as_string()
    return json_record


def make_new_collection_json(collection_json: dict, user_id: str) -> dict:
    json_record = {}
    json_record['dateAddedToDynamo'] = datetime.datetime.fromtimestamp(collection_json.get('created') / 1000).isoformat()
    json_record['dateModifiedInDynamo'] = datetime.datetime.fromtimestamp(collection_json.get('updated') / 1000).isoformat()
    json_record['description'] = collection_json.get('description')
    json_record['featuredCollection'] = False
    json_record['GSI1PK'] = 'PORTFOLIOCOLLECTION'
    json_record['GSI1SK'] = 'PORTFOLIOCOLLECTION#' + format_key_value(collection_json.get('uuid'))
    json_record['GSI2PK'] = 'PORTFOLIOCOLLECTION'
    json_record['GSI2SK'] = 'PUBLIC#' + format_key_value(collection_json.get('uuid'))
    json_record['highlightedCollection'] = False
    json_record['imageUri'] = collection_json.get('image')
    json_record['layout'] = collection_json.get('layout')
    json_record['PK'] = 'PORTFOLIO'
    json_record['portfolioCollectionId'] = format_key_value(collection_json.get('uuid'))
    json_record['portfolioUserId'] = user_id
    json_record['privacy'] = 'public'
    json_record['SK'] = 'USER#' + format_key_value(user_id) + '#' + format_key_value(collection_json.get('uuid'))
    json_record['title'] = collection_json.get('title')
    json_record['TYPE'] = 'PortfolioCollection'
    json_record['migratedDate'] = get_iso_date_as_string()
    return json_record


def make_new_item_json(existing_item_json: dict, user_id: str) -> dict:
    json_record = {}
    json_record['annotation'] = ''
    if existing_item_json.get('annotation') != '{{NULL}}':
        json_record['annotation'] = existing_item_json.get('annotation')
    json_record['dateAddedToDynamo'] = datetime.datetime.fromtimestamp(existing_item_json.get('created') / 1000).isoformat()
    json_record['dateModifiedInDynamo'] = datetime.datetime.fromtimestamp(existing_item_json.get('updated') / 1000).isoformat()
    json_record['description'] = ''
    json_record['GSI1PK'] = 'PORTFOLIOITEM'
    internal_item_id = existing_item_json.get('link').replace('item/', '')
    json_record['GSI1SK'] = 'INTERNALITEM#' + format_key_value(internal_item_id)
    json_record['imageUri'] = existing_item_json.get('image')
    json_record['internalItemId'] = internal_item_id
    json_record['itemType'] = 'internal'
    json_record['PK'] = 'PORTFOLIO'
    json_record['portfolioCollectionId'] = format_key_value(existing_item_json.get('collectionId'))
    json_record['portfolioItemId'] = internal_item_id
    json_record['portfolioUserId'] = user_id
    json_record['sequence'] = int(existing_item_json.get('displayOrder', 0))
    json_record['SK'] = 'USER#' + format_key_value(user_id) + '#' + format_key_value(existing_item_json.get('collectionId')) + '#' + format_key_value(internal_item_id)
    json_record['title'] = existing_item_json.get('title')
    json_record['TYPE'] = 'PortfolioItem'
    json_record['uri'] = existing_item_json.get('manifest')
    json_record['migratedDate'] = get_iso_date_as_string()
    return json_record


def create_json_records_for_collection(collection_uuid: str) -> list:
    old_results = []
    new_results = []
    existing_collection_json = read_existing_collection(collection_uuid)  # could use same uuid value as collectionId
    old_results.append(existing_collection_json)
    user_uuid = existing_collection_json.get('userId')
    existing_user_json = read_existing_user(user_uuid)
    old_results.append(existing_user_json)
    new_user_json = make_new_user_json(existing_user_json)  # good
    new_results.append(new_user_json)
    portfolio_user_id = new_user_json.get('portfolioUserId')
    new_collection_json = make_new_collection_json(existing_collection_json, portfolio_user_id)
    new_results.append(new_collection_json)
    all_existing_items_json = read_all_existing_items_for_a_collection(collection_uuid)
    for existing_item_json in all_existing_items_json:
        old_results.append(existing_item_json)
        new_item_json = make_new_item_json(existing_item_json, portfolio_user_id)
        new_results.append(new_item_json)
    # print('all_existing_items =', len(all_existing_items_json))
    return new_results, old_results


def capture_existing_collection_content(collections_of_interest_list: list, old_content_file_name: str, new_content_file_name: str):
    all_new_results = []
    all_old_results = []

    for col in collections_of_interest_list:
        new_results, old_results = create_json_records_for_collection(col)
        all_new_results.extend(new_results)
        all_old_results.extend(old_results)

    with open(new_content_file_name, 'w') as output_file:
        json.dump(all_new_results, output_file, indent=2)

    with open(old_content_file_name, 'w') as output_file:
        json.dump(all_old_results, output_file, indent=2, default=str)


def save_collection_content_to_new_table(table_name: str, new_content_file_name: str):
    with open(new_content_file_name, 'r', encoding='utf-8') as json_file:
        new_content_list = json.load(json_file)

    table = boto3.resource('dynamodb').Table(table_name)
    with table.batch_writer() as batch:
        for record_to_insert in new_content_list:
            batch.put_item(Item=record_to_insert)
    return


def test(identifier=""):
    """ test exection """
    # print(find_item_records_with_images('steve-manifest-websiteMetadata470E321C-1D6R3LX7EI284'))

    # testlibnd_tables = [
    #     'steve-manifest-websiteMetadata470E321C-1D6R3LX7EI284',
    #     'jon-test-manifest-websiteMetadata470E321C-ZCSU70JC12I0',
    #     'jon-prod-manifest-websiteMetadata470E321C-8NG755QB2S5I',
    #     'sm-test-manifest-websiteMetadata470E321C-E5FXU8HUIWQG',
    #     'sm-prod-manifest-websiteMetadata470E321C-HO7FZQXZXI8M',
    #     'testlib-prod-manifest-websiteMetadata470E321C-1XA9OOG7PJWEE',
    #     'testlib-test-manifest-websiteMetadata470E321C-1XQ2EFEWM3UXZ',
    #     'mlk-manifest-websiteMetadata470E321C-1M04DW0EXCC91'
    # ]
    libnd_tables = [
        'marbleb-prod-manifest-websiteMetadata470E321C-5EJSG31E16Z7',
        'marbleb-test-manifest-websiteMetadata470E321C-JJG277N1OMMC'
    ]

    # collections_of_interest_list = ['e899e792-d26a-4ddf-859f-35f1ea0d1143', '85f5e244-0301-46bc-8ee7-9b4fba9cb0e4', '4314ab19-3ef5-4953-99d2-f54215e154b0', 'dc496e00-1610-465b-9eee-1203dc2e62fb']
    # old_content_file_name = 'old_portfolio_results.json'
    new_content_file_name = 'new_portfolio_results.json'
    # capture_existing_collection_content(collections_of_interest_list, old_content_file_name, new_content_file_name)
    for table_name in libnd_tables:
        save_collection_content_to_new_table(table_name, new_content_file_name)
