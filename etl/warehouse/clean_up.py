"""class for reset and removeing databases"""
from asyncio import wait

from psycopg2 import Error, connect
from psycopg2.extras import DictCursor

from .local_types import Connection, Databases


class Cleaner:
    """class responsible for reseting all databases"""

    def __init__(self, con: Connection) -> None:
        connection = connect(
            dbname="postgres", host=con.host, user=con.user, password=con.password
        )
        connection.set_session(autocommit=True)
        self.cursor = connection.cursor(cursor_factory=DictCursor)

    async def reset_databases(self, databases: Databases) -> None:
        """reset all databases to empty"""
        try:
            await wait([self.drop_databases(db) for db in databases])
            await wait([self.create_databases(db) for db in databases])
        except Error as err:
            print(err)

    async def create_databases(self, database) -> None:
        """create database"""
        self.cursor.execute('CREATE DATABASE "{0}";'.format(database))

    async def drop_databases(self, database: str) -> None:
        """disconnect all connections and drop database"""
        try:
            await self.disconnect(database)
            self.cursor.execute('DROP DATABASE IF EXISTS "{0}";'.format(database))
        except Error as err:
            print(err)

    async def disconnect(self, database: str) -> None:
        """kill all connects to a database"""
        try:
            self.cursor.execute(
                """SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = '{0}'
                AND pid <> pg_backend_pid();""".format(
                    database
                )
            )
        except Error:
            pass

    async def close(self, *args) -> None:
        """close connection to database and remove specific dbs"""
        await wait([self.drop_databases(arg) for arg in args])
        self.cursor.connection.close()
