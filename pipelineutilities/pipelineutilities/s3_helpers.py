import boto3
import json
from os.path import basename
from os import remove


def get_matching_s3_objects(bucket, prefix="", suffix=""):
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {'Bucket': bucket}

    # We can pass the prefix directly to the S3 API.  If the user has passed
    # a tuple or list of prefixes, we go through them one by one.
    if isinstance(prefix, str):
        prefixes = (prefix, )
    else:
        prefixes = prefix

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix
        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                return
            for obj in contents:
                key = obj["Key"]
                if key.endswith(suffix):
                    yield obj


class InprocessBucket():
    def __init__(self, id, config):
        self.id = id
        self.process_bucket = config['process-bucket']
        self.basepath = config['process-bucket-read-basepath'] + "/" + self.id
        self.manifest_url = config['manifest-server-base-url']

    def write_manifest(self, data):
        path = self.basepath + "/metadata/manifest/index.json"
        write_s3_json(self.process_bucket, path, data)

    def write_sub_manifest(self, data):
        path = data.get('id')
        path = path.replace(self.manifest_url + "/", '')
        path = path.replace(self.id, 'metadata')
        path = path + "/index.json"
        path = self.basepath + "/" + path
        # .replace(self.id, 'metadata') + "/index.json"
        write_s3_json(self.process_bucket, path, data)

    def write_collection(self, data):
        path = self.basepath + "/metadata/collection/index.json"
        write_s3_json(self.process_bucket, path, data)

    def write_schema_json(self, data):
        path = self.basepath + "/metadata/index.json"
        write_s3_json(self.process_bucket, path, data)

    def write_mets(self, data):
        path = self.basepath + "/metadata/mets.xml"
        write_s3_json(self.process_bucket, path, data)

    def write_data_csv(self, csv):
        path = self.basepath + "/" + id + ".csv"
        write_s3_file(self.process_bucket, path, csv)

    def write_nd_json(self, data):
        path = self.basepath + "/metadata/nd/index.json"
        write_s3_json(self.process_bucket, path, data)


def read_s3_file_content(s3Bucket, s3Path):
    content_object = boto3.resource('s3').Object(s3Bucket, s3Path)
    return content_object.get()['Body'].read().decode('utf-8')


def read_s3_json(s3Bucket, s3Path):
    return json.loads(read_s3_file_content(s3Bucket, s3Path))


def read_s3_xml(s3Bucket, s3Path):
    return read_s3_file_content(s3Bucket, s3Path)


def write_s3_file(s3Bucket, s3Path, file):
    s3 = boto3.resource('s3')
    s3.Object(s3Bucket, s3Path).put(Body=file)


def write_xml_file(s3Bucket, s3Path, file):
    s3 = boto3.resource('s3')
    s3.Object(s3Bucket, s3Path).put(Body=file, ContentType='application/xml')


def write_s3_json(s3Bucket, s3Path, json_hash):
    s3 = boto3.resource('s3')
    s3.Object(s3Bucket, s3Path).put(Body=json.dumps(json_hash), ContentType='text/json')


def s3_copy_data(dest_bucket, dest_key, src_bucket, src_key, **kwargs):
    """
    Copies S3 data from one key to another
    Args:
        dest_bucket: S3 bucket to copy data to
        dest_key: destination data location
        src_bucket: S3 bucket to copy data from
        src_key: source data location
    """
    s3 = boto3.resource('s3')
    dest_bucket = s3.Bucket(dest_bucket)
    from_source = {
        'Bucket': src_bucket,
        'Key': src_key
    }
    extra = kwargs.get('extra', {})
    dest_bucket.copy(from_source, dest_key, ExtraArgs=extra)


def delete_file(s3Bucket, s3Path):
    return boto3.client('s3').delete_object(
        Bucket=s3Bucket,
        Key=s3Path,
    )


def upload_json(s3Bucket, s3Path, json_data) -> None:
    local_file = f"/tmp/{basename(s3Path)}"
    with open(local_file, 'w') as outfile:
        json.dump(json_data, outfile)
    upload_file(s3Bucket, s3Path, local_file)
    remove(local_file)


def upload_file(s3Bucket, s3Path, local_file):
    boto3.resource('s3').Bucket(s3Bucket).upload_file(local_file, s3Path)
