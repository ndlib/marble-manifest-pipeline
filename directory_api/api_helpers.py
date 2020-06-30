import json


def error(code):
    return {
        "statusCode": code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
        },
    }


def success(data):
    return {
        "statusCode": 200,
        "body": json.dumps(data),
        "headers": {
            "Access-Control-Allow-Origin": "*",
        },
    }
