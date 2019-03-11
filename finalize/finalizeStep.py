import json
import os
import boto3
from botocore.errorfactory import ClientError


class finalizeStep():
    # class constructor
    def __init__(self, id, eventConfig):
        self.id = id
        self.config = eventConfig
        self.manifestMetadata = self.readEventData(id)
        self.error = []

    def run(self):
        if self.success():
            self.movePyramids()
            self.moveManifest()
            self.saveLastRun()
            self.saveIndexMetadata()

        if self.config['notify-on-finished']:
            self.notify()

    def success(self):
        if len(self.manifestMetadata["errors"]) > 0:
            return False

        return True

    def movePyramids(self):
        print("movePyramids")

        s3 = boto3.resource('s3')

        from_bucket = s3.Bucket(self.config["process-bucket"])
        to_bucket = s3.Bucket(self.config["image-server-bucket"])

        from_path = self.config["process-bucket-write-basepath"] + "/" + self.id + "/images/"
        all_objects = from_bucket.object_versions.filter(Prefix=from_path, Delimiter="/")

        for o in all_objects:
            copy_source = {
                'Bucket': self.config["process-bucket"],
                'Key': o.object_key
            }
            other_key = self.config["image-server-bucket-basepath"] + self.id + "/" + os.path.basename(o.object_key)
            to_bucket.copy(copy_source, other_key)
        return

    def moveManifest(self):
        s3 = boto3.resource('s3')
        copy_source = {
            'Bucket': self.config["process-bucket"],
            'Key': self.config["process-bucket-write-basepath"] + "/" + self.id + "/manifest/index.json"
        }

        bucket = s3.Bucket(self.config["manifest-server-bucket"])
        print(copy_source)

        other_key = self.test_basepath(self.config["manifest-server-bucket-basepath"]) \
            + self.id + "/manifest/index.json"
        bucket.copy(copy_source, other_key, ExtraArgs={'ACL': 'public-read'})

        return

    def saveLastRun(self):
        print("Save Last Run")
        s3 = boto3.resource('s3')

        from_path = self.config["process-bucket-read-basepath"] + "/" + self.id + "/"
        # all the items in the process directory for the id
        try:
            objects = self._list_s3_obj_by_dir(from_path, self.config["process-bucket"])

            for s3obj in objects:
                copy_src = {'Bucket': self.config["process-bucket"], 'Key': s3obj}
                dest_key = self.config["process-bucket-write-basepath"] + "/" \
                    + self.id + "/lastSuccessfullRun/" + s3obj[len(from_path):]
                s3.Object(self.config["process-bucket"], dest_key).copy_from(CopySource=copy_src)
        except Exception as e:
            print(e)

    def _list_s3_obj_by_dir(self, s3dir, bucket):
        s3 = boto3.client('s3')
        params = {
            'Bucket': bucket,
            'Prefix': s3dir,
            'StartAfter': s3dir,
        }
        attempt = 0
        keys = []
        while ('ContinuationToken' in params) or (attempt == 0):
            attempt += 1
            objects = s3.list_objects_v2(**params)
            for content in objects['Contents']:
                # skip 'folders'
                if content['Key'].endswith('/'):
                    continue
                keys.append(content['Key'])
            # grab more objects to process if necessary(max 1,000/request)
            if objects['IsTruncated']:
                params['ContinuationToken'] = objects['NextContinuationToken']
            else:
                params.pop('ContinuationToken', None)
        return keys

    def saveIndexMetadata(self):
        print("Save Index Metadata")
        s3 = boto3.resource('s3')

        bucket = self.config["process-bucket"]
        key = self.config["process-bucket-write-basepath"] + "/" + self.id + "/stepFunctionsRunMetadata.json"
        s3.Object(bucket, key).put(Body=json.dumps(self.manifestMetadata))

        return

    def notify(self):
        # Emails must be verified or whitelisted by SES
        RECIPIENTS = self.config['notify-on-finished'].split(",")
        SENDER = "noreply@nd.edu"
        AWS_REGION = "us-east-1"

        # The subject line for the email.
        SUBJECT = self.id + " Manifest Pipeline Complete"

        # The email body for recipients with non-HTML email clients.
        BODY_TEXT = ("The manifest pipeline has completed processing " + self.id)

        # The HTML body of the email.
        BODY_HTML = """<html>
        <head></head>
        <body>
          <h1>""" + self.id + """ manifest pipeline has completed</h1>
          <p>CSVs and images have been processed through the pipeline.</p>
          <ul>
            <li><a href=\"""" + self._event_manifest_url() + """\">Manifest</a></li>
            <li><a href=\"""" + self._event_imageviewer_url() + """\">Image Viewer</a></li>
            <li><a href=\"""" + self._event_imageviewer_url(True) + """\">Image Viewer - Large</a></li>
          </ul>
        </body>
        </html>"""

        CHARSET = "UTF-8"
        client = boto3.client('ses', region_name=AWS_REGION)
        try:
            # Provide the contents of the email.
            response = client.send_email(
                Destination={
                    'ToAddresses': RECIPIENTS,
                },
                Message={
                    'Body': {
                        'Html': {
                            'Charset': CHARSET,
                            'Data': BODY_HTML,
                        },
                        'Text': {
                            'Charset': CHARSET,
                            'Data': BODY_TEXT,
                        },
                    },
                    'Subject': {
                        'Charset': CHARSET,
                        'Data': SUBJECT,
                    },
                },
                Source=SENDER
            )
        # Display an error if something goes wrong.
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print("Email sent! Message ID:"),
            print(response['MessageId'])

        return

    def _event_manifest_url(self):
        return 'https://s3.amazonaws.com/' + self.config['manifest-server-bucket'] \
            + '/' + self.id + '/manifest/index.json'

    def _event_imageviewer_url(self, universalviewer=False):
        url = 'https://image-viewer.library.nd.edu/'
        if universalviewer:
            url += 'universalviewer/index.html#'
        url += '?manifest=' + self._event_manifest_url()
        return url

    def test_basepath(self, basepath):
        if (basepath):
            return basepath + "/"

        return ""

    # read event data
    def readEventData(self, event_id):
        remote_file = self.config['process-bucket-read-basepath'] + "/" \
            + event_id + "/" + self.config["event-file"]
        content_object = boto3.resource('s3').Object(self.config['process-bucket'], remote_file)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        return json.loads(file_content).get('data')
