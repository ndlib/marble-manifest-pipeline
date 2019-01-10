import json, csv, os, glob
import boto3
from botocore.errorfactory import ClientError

class finalizeStep():
    # class constructor

    def __init__(self, id, manifestMetadata):
        print("hi")
        self.id = id
        self.manifestMetadata = manifestMetadata
        self.error = []

    def run(self):
        print("bye")
        print(self.id)
        print(self.manifestMetadata)
        if self.success:
            self.movePyramids()
            self.moveManifest()

        self.notify()

    def success(self):
        if len(self.manifestMetadata["errors"]) > 0
            return false

        return true

    def movePyramids(self):
        return []

    def moveManifest(self):
        return

    def notify(self):
        return
