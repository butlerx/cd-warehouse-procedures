""" restore from backups"""
from os import environ, makedirs, path
from shlex import split
from subprocess import DEVNULL, STDOUT, Popen

from boto3 import Session
from botocore import exceptions

from .config import AWS_S3, Postgres


class S3:
    """restore from s3"""

    def __init__(
        self, con: Postgres, aws: AWS_S3, db_path: str, dev: bool = False
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
                    aws_access_key_id=self._aws.access,
                    aws_secret_access_key=self._aws.secret,
                )
                .resource("s3")
                .Bucket(self._aws.bucket)
            )
            obj = sorted(
                bucket.objects.filter(Prefix=f"zen{database}"),
                key=lambda s3_object: s3_object.last_modified,
                reverse=True,
            )[0]
            print(f"Restoring from {obj.key}")
            bucket.download_file(obj.key, f"{self._path}/{database}.tar.gz")
        except exceptions.ClientError as err:
            if err.response["Error"]["Code"] == "404":
                pass
            else:
                raise

    async def restore_db(self, name: str, database: str) -> None:
        """restore a pg backup stored in specified folder"""
        directory = "f{self._path}/{name}"
        if not path.exists(directory):
            makedirs(directory)
        tar = Popen(
            split(f"tar xvf {directory}.tar.gz -C {directory}"),
            shell=False,
            stdout=DEVNULL,
            stderr=STDOUT,
        )
        pg_env = environ.copy()
        pg_path = pg_env["PATH"]
        pg_env["PATH"] = f"/usr/sbin:/sbin:{pg_path}"
        pg_env["PGPASSWORD"] = self._con.password
        tar.wait()
        postgres = Popen(
            split(
                f"pg_restore -c --if-exists -w -h {self._con.host} -d {database} -U {self._con.user} {directory}/backup_dump"
            ),
            shell=False,
            stderr=STDOUT,
            env=pg_env,
        )
        postgres.wait()
