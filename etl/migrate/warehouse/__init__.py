"""class for reset and removeing databases"""
from asyncio import wait

from psycopg2 import Error, connect
from psycopg2.extras import DictCursor

from ..config import AWS_S3, Databases, Postgres
from .backup import S3


class Store:
    """class responsible for re-storeing all databases"""

    def __init__(self, con: Postgres, aws: AWS_S3, dev: bool, db_path: str) -> None:
        connection = connect(
            dbname="postgres", host=con.host, user=con.user, password=con.password
        )
        connection.set_session(autocommit=True)
        self.cursor = connection.cursor(cursor_factory=DictCursor)
        self._aws = aws
        self._con = con
        self._dev = dev
        self._path = db_path

    async def restore(self, databases: Databases):
        """restore databases from backups"""
        try:
            await wait([self.drop_database(db) for db in databases])
            await wait([self.create_database(db) for db in databases])
            print("Databases Reset")
            aws_s3 = S3(self._con, self._aws, self._path, self._dev)
            await wait(
                [
                    aws_s3.download_db(name, db)
                    for name, db in databases._asdict().items()
                ]
            )
        except Error as err:
            print(err)

    async def create_database(self, database) -> None:
        """create database"""
        self.cursor.execute(f'CREATE DATABASE "{database}";')

    async def drop_database(self, database: str) -> None:
        """disconnect all connections and drop database"""
        try:
            self.cursor.execute(
                """SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = '{database}'
                AND pid <> pg_backend_pid();""".format()
            )
        except Error:
            pass
        finally:
            self.cursor.execute(f'DROP DATABASE IF EXISTS "{database}"')

    async def close(self, *args) -> None:
        """close connection to database and remove specific dbs"""
        await wait([self.drop_database(arg) for arg in args])
        self.cursor.connection.close()
