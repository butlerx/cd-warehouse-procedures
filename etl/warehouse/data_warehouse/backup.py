""" restore from backups"""
from collections import namedtuple
from os import environ, makedirs, path
from shlex import split
from subprocess import DEVNULL, STDOUT, Popen

from boto3 import Session
from botocore import exceptions
from warehouse.local_types import Connection

AWS = namedtuple("AWS", "bucket access_key secret_key")


class S3:
    """restore from s3"""

    def __init__(
        self, con: Connection, aws: AWS, db_path: str, dev: bool = False
    ) -> None:
        self._path = db_path
        self._con = con
        self._aws = aws
        self._dev = dev

    async def download_db(self, name: str, database: str) -> None:
        """download if not in dev mode backups and restore from them"""
        print("Restoring db", name)
        if not self._dev:
            await self.download(name)
        await self.restore_db(name, database)

    async def download(self, database: str) -> None:
        """Download database from backup from s3"""
        try:
            bucket = (
                Session(
                    aws_access_key_id=self._aws.access_key,
                    aws_secret_access_key=self._aws.secret_key,
                )
                .resource("s3")
                .Bucket(self._aws.bucket)
            )
            obj = sorted(
                bucket.objects.filter(Prefix="zen{}".format(database)),
                key=lambda s3_object: s3_object.last_modified,
                reverse=True,
            )[0]
            print("Restoring from {}".format(obj.key))
            bucket.download_file(obj.key, "{0}/{1}.tar.gz".format(self._path, database))
        except exceptions.ClientError as err:
            if err.response["Error"]["Code"] == "404":
                pass
            else:
                raise

    async def restore_db(self, name: str, database: str) -> None:
        """restore a pg backup stored in specified folder"""
        directory = "{}/{}".format(self._path, name)
        if not path.exists(directory):
            makedirs(directory)
        tar = Popen(
            split("tar xvf {0}.tar.gz -C {0}".format(directory)),
            shell=False,
            stdout=DEVNULL,
            stderr=STDOUT,
        )
        pg_env = environ.copy()
        pg_env["PATH"] = "/usr/sbin:/sbin:{}".format(pg_env["PATH"])
        pg_env["PGPASSWORD"] = self._con.password
        tar.wait()
        postgres = Popen(
            split(
                "pg_restore -c --if-exists -w -h {0} -d {1} -U {2} {3}/backup_dump".format(
                    self._con.host, database, self._con.user, directory
                )
            ),
            shell=False,
            stderr=STDOUT,
            env=pg_env,
        )
        postgres.wait()
