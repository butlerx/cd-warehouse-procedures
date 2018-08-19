"""interacting with s3"""

from boto3 import Session
from botocore import exceptions

from .aws import AWS


async def download(database: str, aws: AWS, path: str) -> None:
    """Download database from backup from s3"""
    try:
        session = Session(
            aws_access_key_id=aws.access_key, aws_secret_access_key=aws.secret_key
        )
        aws_s3 = session.resource("s3")
        bucket = aws_s3.Bucket(aws.bucket)
        s3_backups = bucket.objects.filter(Prefix="zen{}".format(database))
        obj = sorted(
            s3_backups, key=lambda s3_object: s3_object.last_modified, reverse=True
        )[0]
        print("Restoring from {}".format(obj.key))
        bucket.download_file(
            obj.key, "{path}/{file}.tar.gz".format(path=path, file=database)
        )
    except exceptions.ClientError as err:
        if err.response["Error"]["Code"] == "404":
            pass
        else:
            raise
