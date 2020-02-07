import boto3
import os
import json
from botocore.errorfactory import ClientError
from dependencies.pipelineutilities.s3_helpers \
    import get_matching_s3_objects, s3_copy_data, read_s3_file_content, delete_file


class FinalizeStep():
    def __init__(self, id, eventConfig):
        self.id = id
        self.config = eventConfig
        self.error = []

    def run(self):
        if self.success():
            self.move_pyramids()
            self.remove_obsolete_pyramids()
            self.move_metadata()
        self.notify()

    def success(self):
        return True
        if self.error:
            return False
        return True

    def move_pyramids(self):
        src_bucket = self.config['process-bucket']
        dest_bucket = self.config['image-server-bucket']
        src_path = f"{self.config['process-bucket-read-basepath']}/{self.id}/images/"
        img_data_path = f"{self.config['process-bucket-read-basepath']}/{self.id}/image_data.json"

        img_data = json.loads(read_s3_file_content(src_bucket, img_data_path))
        all_objects = get_matching_s3_objects(src_bucket, src_path)
        for obj in all_objects:
            # only copy those files that have changed
            file = os.path.basename(obj['Key'])
            filename = file.rsplit('.', maxsplit=1)[0]
            reason = img_data.get(filename, {}).get('reason', '')
            if reason.startswith('no changes'):
                continue
            dest_key = f"{self.id}/{file}"
            s3_copy_data(dest_bucket, dest_key, src_bucket, obj['Key'])
        return

    def remove_obsolete_pyramids(self):
        src_bucket = self.config['process-bucket']
        dest_bucket = self.config['image-server-bucket']
        src_path = f"{self.config['process-bucket-read-basepath']}/{self.id}/images"
        img_data = f"{self.config['process-bucket-read-basepath']}/{self.id}/image_data.json"
        latest_images = get_matching_s3_objects(src_bucket, src_path)
        public_images = get_matching_s3_objects(dest_bucket, self.id)
        latest = []
        public = []
        manifest = []

        for image in public_images:
            public.append(os.path.basename(image['Key']))
        for image in latest_images:
            latest.append(os.path.basename(image['Key']))

        # delete images from image bucket not in proces bucket
        deletion_list = list(set(public) - set(latest))
        for image in deletion_list:
            print(f"Removing - {self.id}/{image} from {dest_bucket}")
            delete_file(dest_bucket, f"{self.id}/{image}")

        manifest_data = json.loads(read_s3_file_content(src_bucket, img_data))
        for image in manifest_data:
            manifest.append(f"{image}.tif")

        # delete images from process bucket not in manifest
        deletion_list = list(set(latest) - set(manifest))
        for image in deletion_list:
            print(f"Removing - {src_path}/{image} from {src_bucket}")
            delete_file(src_bucket, f"{self.id}/{image}")

    def move_metadata(self):
        src_bucket = self.config['process-bucket']
        dest_bucket = self.config['manifest-server-bucket']
        src_path = f"{self.config['process-bucket-read-basepath']}/{self.id}/metadata/"

        all_objects = get_matching_s3_objects(src_bucket, src_path)
        for obj in all_objects:
            dest_key = obj['Key'].replace('metadata/', '').replace('process/', '')
            s3_copy_data(dest_bucket, dest_key, src_bucket, obj['Key'])
        return

    def notify(self):
        return

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
