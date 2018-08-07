#! /usr/bin/env python3
"""main function"""

from __future__ import print_function

import argparse
import asyncio
import json
import sys
from typing import Dict

import psycopg2
import psycopg2.extras
from clean_up import Cleaner, Connection, Datebases
from migrate import Convertor, setup_warehouse
from restore import restore_db
from s3 import AWS, download


class Warehouse():
    """main class for parseing config and running migration"""

    def __init__(self, config: Dict) -> None:
        self.con = Connection(
            host=config['postgres']['host'],
            password=config['postgres']['password'],
            user=config['postgres']['user'])
        self.databases = Datebases(
            warehouse=config['databases']['dw'],
            dojos=config['databases']['dojos'],
            events=config['databases']['events'],
            users=config['databases']['users'])
        self.aws = AWS(bucket=config['s3']['bucket'],
                       access_key=config['s3']['access'],
                       secret_key=config['s3']['secret'])
        self.cleaner = Cleaner(self.con)

    async def get(self, name: str, database: str, dev: bool) -> None:
        print('Restoring db', name)
        if not dev:
            download(name, self.aws)
        restore_db(self.con, database, name)

    def main(self, dev: bool=False):
        """main function"""
        try:
            # Postgres
            self.cleaner.reset_databases(self.databases)
            print("Databases Reset")

            convertor = Convertor(self.databases, self.con)

            # Download and restore db in parallel
            loop = asyncio.get_event_loop()
            loop.run_until_complete(
                asyncio.gather(
                    convertor.setup_warehouse(),
                    self.get('dojos', self.databases.dojos, dev),
                    self.get('events', self.databases.events, dev),
                    self.get('users', self.databases.users, dev), ))
            loop.close()

            convertor.connect()
            convertor.migrate_db()
            print("Databases Migrated")
            convertor.disconnect()
            self._exit(0)
        except psycopg2.Error as err:
            print(err)
            self._exit(1)

    def _exit(self, exit_code: int):
        self.cleaner.close(self.databases.dojos, self.databases.events,
                           self.databases.users)
        print("Removed {}, {} and {}".format(
            self.databases.dojos, self.databases.events, self.databases.users))
        sys.exit(exit_code)


def __cli():
    parser = argparse.ArgumentParser(
        description='migrate production databases backups to datawarehouse')
    parser.add_argument(
        '--dev', action='store_true', help='dev mode to use local backups')
    args = parser.parse_args()
    with open('./config/config.json') as data:
        warehouse = Warehouse(json.load(data))
        warehouse.main(args.dev)


if __name__ == '__main__':
    cli()
