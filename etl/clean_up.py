"""class for reset and removeing databases"""
from collections import namedtuple

import psycopg2
import psycopg2.extras

Databases = namedtuple('Databases', 'warehouse dojos events users')
Connection = namedtuple('Connection', 'host user password')


class Cleaner():
    """class responsible for reseting all databases"""

    def __init__(self, con: Connection) -> None:
        dw_setup = psycopg2.connect(
            dbname='postgres',
            host=con.host,
            user=con.user,
            password=con.password)
        dw_setup.set_session(autocommit=True)
        self.cursor = dw_setup.cursor()

    def reset_databases(self, databases: Databases) -> None:
        """reset all databases to empty"""
        try:
            for database in databases:
                self.drop_databases(database)
                self.create(database)
        except (psycopg2.Error) as err:
            print(err)

    def create(self, database):
        """create database"""
        self.cursor.execute('CREATE DATABASE "{0}";'.format(database))

    def drop_databases(self, database: str) -> None:
        """disconnect all connections and drop database"""
        try:
            self.disconnect(database)
            self.cursor.execute('DROP DATABASE IF EXISTS "{0}"'.format(
                database))
        except (psycopg2.Error) as err:
            print(err)

    def disconnect(self, database: str) -> None:
        """kill all connects to a database"""
        try:
            self.cursor.execute('''
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = '{0}'
                  AND pid <> pg_backend_pid();
                '''.format(database))
        except psycopg2.Error:
            pass

    def close(self, *kwargs) -> None:
        """close connection to database and remove specific dbs"""
        for database in kwargs:
            self.drop_databases(database)
        self.cursor.connection.close()
