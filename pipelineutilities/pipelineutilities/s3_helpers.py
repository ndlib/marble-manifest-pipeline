import boto3
import botocore
from datetime import datetime, date
import json
from os.path import basename
from os import remove
import hashlib
import re


def get_matching_s3_objects(bucket: str, prefix: str = "", suffix: str = "") -> list:
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    s3 = s3_client()
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
        path = re.sub(r'^' + self.id + '[/]', 'metadata/', path)        path = path + "/index.json"
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

    def write_nd_json(self):
        to_path = self.basepath + "/metadata/nd/index.json"
        from_path = "json/" + self.id + ".json"

        s3_copy_data(self.process_bucket, to_path, self.process_bucket, from_path)


def read_s3_file_content(s3_bucket: str, s3_key: str) -> str:
    """
    Returns an s3 file as a decoded utf-8 string
    Note: Does not trap botocore.exceptions.ClientError errors.
    Use s3_file_exists

    :param s3_bucket: Name of the S3 bucket.
    :param s3_key: Key to write to s3.
    """
    resource = s3_resource()
    try:
        return resource.Object(s3_bucket, s3_key).get()['Body'].read().decode('utf-8')

    except botocore.exceptions.ClientError:
        return ""


def read_s3_json(s3_bucket: str, s3_key: str) -> str:
    """
    Returns an s3 json file as dict
    Note: Does not trap botocore.exceptions.ClientError errors.
    Use s3_file_exists

    :param s3_bucket: Name of the S3 bucket.
    :param s3_key: Key to write to s3.
    """
    content = read_s3_file_content(s3_bucket, s3_key)
    if not content:
        return {}

    return json.loads(content)


def write_s3_file(s3_bucket: str, s3_key: str, filedata: str, **kwargs) -> None:
    """
    Writes a string file to s3.
    Tests if the file is already on s3

    :param s3_bucket: Name of the S3 bucket.
    :param s3_key: Key to write to s3.
    :param filedata: String file data to add
    :param kwargs: Additional params to pass to boto object.put
    """
    if not filedata_is_already_on_s3(s3_bucket, s3_key, filedata):
        kwargs['Body'] = filedata

        s3 = s3_resource()
        s3.Object(s3_bucket, s3_key).put(**kwargs)


def write_s3_xml(s3_bucket: str, s3_key: str, filedata: str, **kwargs) -> None:
    """
    Writes a string xml file to s3.
    Tests if the file is already on s3

    :param s3_bucket: Name of the S3 bucket.
    :param s3_key: Key to write to s3.
    :param filedata: String file data to add
    :param kwargs: Additional params to pass to boto object.put
    """
    kwargs['ContentType'] = 'application/xml'
    write_s3_file(s3_bucket, s3_key, filedata, **kwargs)


def write_s3_json(s3_bucket: str, s3_key: str, json_dict: dict, **kwargs) -> None:
    """
    Writes a dict as a json file to s3.
    Tests if the file is already on s3

    :param s3_bucket: Name of the S3 bucket.
    :param s3_key: Key to write to s3.
    :param json_dict: Dict file data to add as json
    :param kwargs: Additional params to pass to boto object.put
    """
    filedata = json.dumps(json_dict, default=json_serial)
    kwargs['ContentType'] = 'text/json'
    write_s3_file(s3_bucket, s3_key, filedata, **kwargs)


def filedata_is_already_on_s3(s3_bucket: str, s3_key: str, filedata: str) -> bool:
    """
    tests if a file and its contents are already on s3 by comparing the etag.
    Note: this method is insuficient for large files. Files over 5GB are checksumed in a different way.
    we are being optinmistic because we don't have files we expect to be that large.
    https://stackoverflow.com/questions/6591047/etag-definition-changed-in-amazon-s3/31086810#31086810

    :param s3_bucket: Name of the S3 bucket.
    :param s3_key: Key to write to s3.
    :param filedata: String file data to add
    """
    obj_dict = s3_file_exists(s3_bucket, s3_key)
    if not obj_dict:
        return False

    etag = (obj_dict['ETag'])
    etag = etag[1:-1]  # strip quotes
    if '-' in etag:
        raise Exception("Etag from aws is in large file format and cant' be tested with our current method.")

    file_etag = md5_checksum(filedata)
    # if the etags match the file is on s3
    return (etag == file_etag)


def md5_checksum(filedata: str) -> str:
    """
    returns the md5 checksum of a string used for testing if the object is already on s3
    Note: this method is insuficient for large files. Files over 5GB are checksumed in a different way.
    we are being optinmistic because we don't have files we expect to be that large.
    https://stackoverflow.com/questions/6591047/etag-definition-changed-in-amazon-s3/31086810#31086810

    :param filedata: String file data to add
    """
    m = hashlib.md5()
    m.update(filedata.encode('utf-8'))

    return m.hexdigest()


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


def delete_s3_key(s3_bucket: str, s3_key: str) -> None:
    """
    Deletes an s3 key
    Note: Does not trap botocore.exceptions.ClientError errors.
    Use s3_file_exists

    :param s3_bucket: Name of the S3 bucket.
    :param s3_key: Key to delete from
    """
    s3 = s3_client()
    return s3.delete_object(
        Bucket=s3_bucket,
        Key=s3_key,
    )


def delete_file(s3Bucket, s3Path):
    """
    Depreciated use delete_s3_key
    """
    s3 = s3_client()
    return s3.delete_object(
        Bucket=s3Bucket,
        Key=s3Path,
    )


def upload_json(s3Bucket, s3Path, json_data) -> None:
    """
    Deptreciated use write_s3_json
    """
    local_file = f"/tmp/{basename(s3Path)}"
    with open(local_file, 'w') as outfile:
        json.dump(json_data, outfile)
    upload_file(s3Bucket, s3Path, local_file)
    remove(local_file)


def upload_file_to_s3(s3_bucket: str, s3_key: str, local_filepath: str) -> bool:
    """
    Uploads as file from the local file system to s3.

    :param s3_bucket: Name of the S3 bucket.
    :param s3_key: Key to delete from
    :param local_filepath: path to the local file
    """
    success = True
    try:
        resource = s3_resource()
        resource.Bucket(s3_bucket).upload_file(local_filepath, s3_key)
    except FileNotFoundError:
        success = False
    except botocore.exceptions.ClientError:
        success = False
    except boto3.exceptions.S3UploadFailedError:
        success = False
    except botocore.exceptions.NoCredentialsError:
        success = False
    return success


def upload_file(s3_bucket: str, s3_key: str, local_filepath: str) -> None:
    """
    Depreciated
    use upload_file_to_s3
    """
    resource = s3_resource()
    resource.Bucket(s3_bucket).upload_file(local_filepath, s3_key)


def s3_file_exists(s3_bucket: str, s3_key: str) -> dict:
    """
    tells you if an object is in s3.
    Returns the head metadata on true
    Traps the client error and returns false if it does not exist
    :param s3_bucket: Name of the S3 bucket.
    :param s3_key: Key to delete from
    """
    s3 = s3_client()
    try:
        return s3.head_object(Bucket=s3_bucket, Key=s3_key)

    except botocore.exceptions.ClientError:
        return False
    except botocore.exceptions.NoCredentialsError:
        return False


def s3_client():
    return boto3.client('s3')


def s3_resource():
    return boto3.resource('s3')


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


# python -c 'from s3_helpers import *; test()'
def test():
    bucket_name = "marble-manifest-prod-processbucket-kskqchthxshg"
    your_key = "json/000297305.json"
    file = '{"sourceSystem": "EmbARK", "title": "Crucifixion", "dateCreated": "ca. 1360", "workType": "Paintings", "medium": "tempera on panel, gold ground", "uniqueIdentifier": "1961.047.004", "sequence": 0, "repository": "museum", "subjects": [{"authority": "IA", "term": "Bible, New Testament", "uri": "http://vocab.getty.edu/page/ia/901000254"}, {"authority": "AAT", "term": "crucifixions", "uri": "http://vocab.getty.edu/aat/300404300"}, {"authority": "AAT", "term": "deaths", "uri": "http://vocab.getty.edu/aat/300151836"}, {"authority": "AAT", "term": "religions", "uri": "http://vocab.getty.edu/aat/300073708"}], "copyrightStatus": "Public domain", "copyrightStatement": "", "access": "Galleries: Ancient, Medieval, and Renaissance Art Gallery", "format": "", "dimensions": "15 1/2 x 5 1/2 in. (39.37 x 13.97 cm)", "dedication": "Gift of the Samuel H. Kress Foundation", "description": "", "creationPlace": {"continent": "", "country": "", "state": "", "county": "", "city": "", "historic": ""}, "id": "1961.047.004", "collectionId": "1961.047.004", "parentId": "root", "creators": [{"attribution": "", "role": "Primary", "fullName": "Puccio di Simone", "nationality": "Italian", "lifeDates": "active ca. 1345 - 1365", "startDate": "ca. 1345", "endDate": "1365", "human": false, "alive": false, "display": "Puccio di Simone (Italian, active ca. 1345 - 1365)"}], "modifiedDate": "2020-06-15T11:01:00Z", "level": "manifest", "apiVersion": 1, "fileCreatedDate": "2020-06-16", "items": [{"id": "1961_047_004-v0001.jpg", "level": "file", "sequence": 1, "title": "1961_047_004-v0001.jpg", "description": "1961_047_004-v0001.jpg", "thumbnail": true, "md5Checksum": "8e912c79ed64b040c52e2a0f8a9782d5", "filePath": "https://drive.google.com/a/nd.edu/file/d/1mcmtmW8M9gCiL6uQ--XUbDSVCgwy9qzw/view", "fileId": "1mcmtmW8M9gCiL6uQ--XUbDSVCgwy9qzw", "modifiedDate": "2019-06-25T17:58:34.840Z", "mimeType": "image/jpeg", "collectionId": "1961.047.004", "parentId": "1961.047.004", "sourceSystem": "EmbARK", "repository": "museum", "apiVersion": 1, "fileCreatedDate": "2020-06-16", "iiifImageUri": "https://image-iiif.library.nd.edu/iiif/2/1961.047.004%2F1961_047_004-v0001", "iiifImageFilePath": "s3://marble-data-broker-publicbucket-1kvqtwnvkhra2/1961.047.004/1961_047_004-v0001", "iiifUri": "https://presentation-iiif.library.nd.edu/1961.047.004/1961_047_004-v0001/canvas", "iiifFilePath": "s3://marble-manifest-prod-manifestbucket-lpnnaj4jaxl5/1961.047.004/1961_047_004-v0001/canvas/index.json", "metsUri": "", "metsFilePath": "", "schemaUri": "", "schemaPath": ""}], "iiifImageUri": "", "iiifImageFilePath": "", "iiifUri": "https://presentation-iiif.library.nd.edu/1961.047.004/manifest", "iiifFilePath": "s3://marble-manifest-prod-manifestbucket-lpnnaj4jaxl5/1961.047.004/manifest", "metsUri": "https://presentation-iiif.library.nd.edu/1961.047.004/mets.xml", "metsFilePath": "s3://marble-manifest-prod-manifestbucket-lpnnaj4jaxl5/1961.047.004/mets.xml", "schemaUri": "https://presentation-iiif.library.nd.edu/1961.047.004", "schemaPath": "s3://marble-manifest-prod-manifestbucket-lpnnaj4jaxl5/1961.047.004/index.json"}'  # noqa

    print(s3_file_exists(bucket_name, your_key))

    # etag_checksum(filename, chunk_size=8 * 1024 * 1024)
