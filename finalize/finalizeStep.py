import json, csv, os, glob
import boto3
from botocore.errorfactory import ClientError

class finalizeStep():
    # class constructor

    def __init__(self, id, manifestMetadata):
        self.id = id
        self.manifestMetadata = manifestMetadata
        self.error = []

    def run(self):
        if self.success():
            self.movePyramids()
            self.moveManifest()
            self.saveLastRun()
            self.saveIndexMetadata()

        self.notify()

    def success(self):
        if len(self.manifestMetadata["errors"]) > 0:
            return False

        return True

    def movePyramids(self):
        print("movePyramids")

        s3 = boto3.resource('s3')

        from_bucket = s3.Bucket(self.manifestMetadata["config"]["process-bucket"])
        to_bucket = s3.Bucket(self.manifestMetadata["config"]["image-server-bucket"])

        from_path = self.manifestMetadata["config"]["process-bucket-write-basepath"] + "/" + self.id + "/images/"
        all_objects = from_bucket.object_versions.filter(Prefix=from_path, Delimiter="/")

        for o in all_objects:
            copy_source = {
                'Bucket': self.manifestMetadata["config"]["process-bucket"],
                'Key': o.object_key
            }
            to_bucket.copy(copy_source, self.manifestMetadata["config"]["image-server-bucket-basepath"] + self.id + "/" + os.path.basename(o.object_key))
        return

    def moveManifest(self):
        print("moveManifest")

        s3 = boto3.resource('s3')
        copy_source = {
          'Bucket': self.manifestMetadata["config"]["process-bucket"],
          'Key': self.manifestMetadata["config"]["process-bucket-write-basepath"] + "/" + self.id + "/manifest/index.json"
        }

        bucket = s3.Bucket(self.manifestMetadata["config"]["manifest-server-bucket"])
        print(copy_source)
        bucket.copy(copy_source, self.test_basepath(self.manifestMetadata["config"]["manifest-server-bucket-basepath"]) + self.id + "/manifest/index.json", ExtraArgs={'ACL':'public-read'})

        return

    def saveLastRun(self):
        print("Save Last Run")
        s3 = boto3.resource('s3')

        from_bucket = s3.Bucket(self.manifestMetadata["config"]["process-bucket"])
        to_bucket = s3.Bucket(self.manifestMetadata["config"]["process-bucket"])

        from_path = self.manifestMetadata["config"]["process-bucket-read-basepath"] + "/" + self.id + "/"
        # all the items in the process directory for the id
        all_objects = from_bucket.object_versions.filter(Prefix=from_path, Delimiter="/")

        for o in all_objects:
            copy_source = {
                'Bucket': self.manifestMetadata["config"]["process-bucket"],
                'Key': o.object_key
            }
            to_bucket.copy(copy_source, self.manifestMetadata["config"]["process-bucket-write-basepath"] + "/" + self.id + "/lastSuccessfullRun/" + os.path.basename(o.object_key))
        return

    def saveIndexMetadata(self):
        print("Save Index Metadata")
        s3 = boto3.resource('s3')

        bucket = self.manifestMetadata["config"]["process-bucket"]
        key = self.manifestMetadata["config"]["process-bucket-write-basepath"] + "/" + self.id + "/stepFunctionsRunMetadata.json"
        s3.Object(bucket, key).put(Body=json.dumps(self.manifestMetadata))

        return

    def notify(self):
        print("nofify")
        RECIPIENT = "jhartzle@nd.edu"
        SENDER = "jon.hartzler@gmail.com"
        AWS_REGION = "us-east-1"

        # The subject line for the email.
        SUBJECT = "Amazon SES Test (SDK for Python)"

        # The email body for recipients with non-HTML email clients.
        BODY_TEXT = ("Amazon SES Test (Python)\r\n"
                     "This email was sent with Amazon SES using the "
                     "AWS SDK for Python (Boto)."
                    )

        # The HTML body of the email.
        BODY_HTML = """<html>
        <head></head>
        <body>
          <h1>Amazon SES Test (SDK for Python)</h1>
          <p>This email was sent with
            <a href='https://aws.amazon.com/ses/'>Amazon SES</a> using the
            <a href='https://aws.amazon.com/sdk-for-python/'>
              AWS SDK for Python (Boto)</a>.</p>
        </body>
        </html>
                    """
        CHARSET = "UTF-8"
        client = boto3.client('ses',region_name=AWS_REGION)
        try:
            #Provide the contents of the email.
            response = client.send_email(
                Destination={
                    'ToAddresses': [
                        RECIPIENT,
                    ],
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

    def test_basepath(self, basepath):
        if (basepath):
            return basepath + "/"

        return ""
