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

        from_path = self.manifestMetadata["config"]["process-bucket-write-basepath"] + "/" + self.id + "/pyramid/"
        all_objects = from_bucket.object_versions.filter(Prefix=from_path, Delimiter="/")

        for o in all_objects:
            copy_source = {
                'Bucket': self.manifestMetadata["config"]["process-bucket"],
                'Key': o.object_key
            }
            to_bucket.copy(copy_source, self.manifestMetadata["config"]["image-server-bucket-basepath"] + "/" + self.id + "/" + os.path.basename(o.object_key))
        return []

    def moveManifest(self):
        print("moveManifest")

        s3 = boto3.resource('s3')
        copy_source = {
          'Bucket': self.manifestMetadata["config"]["process-bucket"],
          'Key': self.manifestMetadata["config"]["process-bucket-write-basepath"] + "/" + self.id + "/manifest/index.json"
        }

        bucket = s3.Bucket(self.manifestMetadata["config"]["manifest-server-bucket"])
        bucket.copy(copy_source, self.test_basepath(self.manifestMetadata["config"]["manifest-server-bucket-basepath"]) + self.id + "/manifest/index.json")

        return

    def notify(self):
        print("nofify")
        return

    def test_basepath(self, basepath):
        if (basepath):
            return basepath + "/"

        return ""
