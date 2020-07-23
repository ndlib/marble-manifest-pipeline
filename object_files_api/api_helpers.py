import json
from datetime import datetime, date


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
        "body": json.dumps(data, default=json_serial),
        "headers": {
            "Access-Control-Allow-Origin": "*",
        },
    }


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))
