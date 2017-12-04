#! /usr/bin/env python3

from __future__ import print_function

import argparse
import asyncio
import json
import sys

import psycopg2
import psycopg2.extras
from clean_up import clean_databases, reset_databases
from migrate import migrate_db, setup_warehouse
from restore import restore_db
from s3 import download

with open('./config/config.json') as data:
    data = json.load(data)

db_host = data['postgres']['host']
db_password = data['postgres']['password']
db_user = data['postgres']['user']
dojos = data['databases']['dojos']
dw = data['databases']['dw']
events = data['databases']['events']
users = data['databases']['users']


async def get(name, dev):
    print('Restoring db', name)
    sys.stdout.flush()
    if (not dev):
        download(name)
    restore_db(db_host, data['databases'][name], db_user, db_password, name)


def main():
    parser = argparse.ArgumentParser(
        description='migrate production databases backups to datawarehouse')
    parser.add_argument('--dev', action='store_true',
                        help='dev mode to use local backups')
    args = parser.parse_args()
    try:
        # Postgres
        dw_setup = psycopg2.connect(
            dbname='postgres',
            host=db_host,
            user=db_user,
            password=db_password)
        dw_setup.set_session(autocommit=True)
        cursor = dw_setup.cursor()
        reset_databases(cursor, dojos, dw, events, users)
        print("Databases Reset")
        sys.stdout.flush()

        # cdDataWarehouse
        dw_conn = psycopg2.connect(
            dbname=dw, host=db_host, user=db_user, password=db_password)
        dw_cursor = dw_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        dw_conn.set_session(autocommit=True)

        # Download and restore db in parallel
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(
            setup_warehouse(dw_cursor),
            get('dojos', args.dev),
            get('events', args.dev),
            get('users', args.dev),
        ))
        loop.close()

        # cp-dojos
        dojos_conn = psycopg2.connect(
            dbname=dojos, host=db_host, user=db_user, password=db_password)
        dojos_cursor = dojos_conn.cursor(
            cursor_factory=psycopg2.extras.DictCursor)
        # cp-events
        events_conn = psycopg2.connect(
            dbname=events, host=db_host, user=db_user, password=db_password)
        events_cursor = events_conn.cursor(
            cursor_factory=psycopg2.extras.DictCursor)
        # cp-users
        users_conn = psycopg2.connect(
            dbname=users, host=db_host, user=db_user, password=db_password)
        users_cursor = users_conn.cursor(
            cursor_factory=psycopg2.extras.DictCursor)

        migrate_db(dw_cursor, users_cursor, dojos_cursor, events_cursor)
        print("Databases Migrated")
        sys.stdout.flush()

        # Close Database connections and delete the dev databases
        dw_cursor.connection.close()
        users_cursor.connection.close()
        events_cursor.connection.close()
        dojos_cursor.connection.close()
        clean_databases(cursor, dojos, events, users)
        print("Removed ", dojos, events, users)
        cursor.connection.close()
        sys.exit(0)
    except (psycopg2.Error) as e:
        print(e)
        clean_databases(cursor, dojos, events, users)
        print("Removed ", dojos, events, users)
        cursor.close
        sys.exit(1)


if __name__ == '__main__':
    main()
