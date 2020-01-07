import manifest_utils as mu
import boto3
from botocore.errorfactory import ClientError


class FinalizeStep():
    def __init__(self, id, eventConfig):
        self.id = id
        self.config = eventConfig
        self.error = []

    def run(self):
        if self.success():
            self.move_pyramids()
            self.move_manifest()
            self.move_schema()
            self.move_mets()
            self.save_last_run()
        self.notify()

    def success(self):
        if self.error:
            return False
        return True

    def move_pyramids(self):
        src_bucket = self.config['process-bucket']
        dest_bucket = self.config['image-server-bucket']
        src_path = f"{self.config['process-bucket-write-basepath']}/{self.id}/images/"

        all_objects = mu.s3_list_obj_by_dir(src_bucket, src_path)
        for obj in all_objects:
            dest_key = f"{self.config['image-server-bucket-basepath']}{self.id}/{obj[len(src_path):]}"
            mu.s3_copy_data(dest_bucket, dest_key, src_bucket, obj)
        return

    def move_manifest(self):
        src_bucket = self.config['process-bucket']
        src_key = f"{self.config['process-bucket-write-basepath']}/{self.id}/manifest/index.json"
        dest_bucket = self.config['manifest-server-bucket']
        dest_key = f"{self.test_basepath(self.config['manifest-server-bucket-basepath'])}{self.id}/manifest/index.json"
        mu.s3_copy_data(dest_bucket, dest_key, src_bucket, src_key, extra={'ACL': 'public-read'})
        return

    def move_schema(self):
        src_bucket = self.config['process-bucket']
        src_key = f"{self.config['process-bucket-write-basepath']}/{self.id}/{self.config['schema-file']}"
        dest_bucket = self.config['manifest-server-bucket']
        dest_key = f"{self.test_basepath(self.config['manifest-server-bucket-basepath'])}{self.id}/index.json"
        mu.s3_copy_data(dest_bucket, dest_key, src_bucket, src_key, extra={'ACL': 'public-read'})
        return

    def move_mets(self):
        if self.config['metadata-source-type'] == 'mets':
            src_bucket = self.config["process-bucket"]
            src_key = f"{self.config['process-bucket-write-basepath']}/{self.id}/mets.xml"
            dest_bucket = self.config["manifest-server-bucket"]
            dest_key = f"{self.test_basepath(self.config['manifest-server-bucket-basepath'])}{self.id}/mets.xml"
            mu.s3_copy_data(dest_bucket, dest_key, src_bucket, src_key, extra={'ACL': 'public-read'})
        return

    def save_last_run(self):
        src_bucket = self.config['process-bucket']
        dest_bucket = self.config['process-bucket']
        src_path = f"{self.config['process-bucket-read-basepath']}/{self.id}/"

        all_objects = mu.s3_list_obj_by_dir(src_bucket, src_path)
        for obj in all_objects:
            dest_key = f"{self.config['process-bucket-write-basepath']}/{self.id}/lastSuccessfullRun/{obj[len(src_path):]}"
            print(dest_key)
            mu.s3_copy_data(dest_bucket, dest_key, src_bucket, obj)
        return

    def notify(self):
        if self.success():
            if self.config.get('notify-on-finish', False):
                recipients = self.config['notify-on-finished'].split(",")
            else:
                recipients = ['jhartzle@nd.edu']

            subject = self.id + " Manifest Pipeline Complete"
            body_text = "The manifest pipeline has completed processing " + self.id
            body_html = """<html>
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
        else:
            recipients = []
            if self.config.get('notify-on-finished', False):
                recipients = self.config.get('notify-on-finished').split(",")
            recipients.append(self.config['troubleshoot-email-addr'])
            subject = "Error processing " + self.id
            if self.error:
                reportable_errs = "<li>" + str(self.error) + "</li>"
            else:
                for err in self.config.get("errors", []):
                    reportable_errs += "<li>" + err + "</li>"
            body_html = """<html>
            <head></head>
            <body>
            <h1>""" + self.id + """ failed with errors!</h1>
            <p>The pipeline failed for the following reasons:</p>
            <ul>""" + reportable_errs + """</ul>
            </body>
            </html>"""
            body_text = "The following errors were encountered processing: " \
                + reportable_errs

        self._send_notification(recipients, subject, body_html, body_text)

        return

    def _send_notification(self, recipients, subject, body_html, body_text):
        SENDER = self.config['noreply-email-addr']
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
        return self.config['manifest-server-base-url'] \
            + "/" + self.id + '/manifest/index.json'

    def _event_imageviewer_url(self, universalviewer=False):
        url = 'https://viewer-iiif.library.nd.edu/'
        if universalviewer:
            url += 'universalviewer/index.html#'
        url += '?manifest=' + self._event_manifest_url()
        return url

    def test_basepath(self, basepath):
        if (basepath):
            return basepath + "/"

        return ""
