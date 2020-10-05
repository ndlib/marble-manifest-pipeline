"""
Report Missing Fields
Finds and reports required fields missing from standard.json
"""
import boto3
from botocore.errorfactory import ClientError
from sentry_sdk import capture_message, push_scope
import re


class ReportMissingFields():
    """ Report Missing Fields Class """
    def __init__(self, config: dict):
        self.config = config

    def process_missing_fields(self, standard_json: dict, sendEmail: bool = True) -> str:
        """ Find and report missing required fields from source systems """
        missing_fields = ''
        source_system = standard_json.get('sourceSystem', '')
        required_fields_for_source_system = self._get_required_fields_for_source_system(source_system)
        if required_fields_for_source_system:
            missing_fields = self.find_missing_fields(standard_json, required_fields_for_source_system.get('required-fields', ''))
            if missing_fields > '':
                missing_fields_notification = standard_json.get('id', '') + ' is missing the following required field(s): \n' + missing_fields + '\n'
                notify_list = required_fields_for_source_system.get('notify', None)
                if sendEmail and notify_list:
                    self.email_missing_fields(notify_list, standard_json.get('id', ''), missing_fields, missing_fields_notification)
                else:
                    _log_missing_field(standard_json.get('sourceSystem', ''), missing_fields_notification)
                # print(missing_fields_notification)
        return missing_fields

    def _get_required_fields_for_source_system(self, source_system):
        return self.config['required-fields-by-source-system'].get(source_system, {})

    def find_missing_fields(self, standard_json: dict, required_fields: dict) -> str:  # object_id: dict, json_object: dict, required_fields: dict) -> str:
        """ Test for missing required fields """
        missing_fields = ''
        for preferred_name, json_path in required_fields.items():
            value = standard_json.get(json_path, None)
            if value == '' or value == {} or value == [] or value is None:
                missing_fields += preferred_name + ' - at json path location ' + json_path + '\n'
        return missing_fields

    def email_missing_fields(self, notify_list: list, item_id: str, missing_fields: str, missing_fields_notification: str):
        regex = r'\n$'
        missing_fields_string = re.sub(regex, '', missing_fields)
        sender_email_address = self.config['noreply-email-addr']
        body_html = """<html>
            <head></head>
            <body>
            <h1>""" + item_id + """ ' is missing the following required field(s): </h1>
            <ul>"""
        for item in missing_fields_string.split('\n'):
            body_html += """<li>""" + item + """</li>"""
        body_html += """
            </ul>
            </body>
            </html>"""
        _send_email(sender_email_address, notify_list, 'MARBLE required fields missing from Source System', body_html, missing_fields_notification)
        return


def _log_missing_field(source_system: str, missing_fields_notification: str):
    """ Log missing field information to sentry """
    with push_scope() as scope:
        scope.set_tag('sourceSystem', source_system)
        scope.set_tag('problem', 'missing_field')
        scope.level = 'warning'
        capture_message(missing_fields_notification)


def _send_email(sender_email_address: str, recipients: list, subject: str, body_html: str, body_text: str):
    AWS_REGION = "us-east-1"
    CHARSET = "UTF-8"
    client = boto3.client('ses', region_name=AWS_REGION)
    try:
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': recipients,
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': body_html,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': body_text,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': subject,
                },
            },
            Source=sender_email_address
        )
    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])
    return
