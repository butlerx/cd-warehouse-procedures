#!/usr/bin/env python

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


def main():
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

        # cdDataWarehouse
        dw_conn = psycopg2.connect(
            dbname=dw, host=db_host, user=db_user, password=db_password)
        dw_cursor = dw_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        dw_cursor.set_session(autocommit=True)
        setup_warehouse(dw_cursor)

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

        download('dojos')
        restore_db(db_host, dojos, db_user, db_password, '/db/dojos.tar.gz')
        download('events')
        restore_db(db_host, events, db_user, db_password, '/db/events.tar.gz')
        download('users')
        restore_db(db_host, users, db_user, db_password, '/db/users.tar.gz')

        migrate_db(dw_cursor, users_cursor, dojos_cursor, events_cursor)
        print("data migrated")

        # Close Database connections and delete the dev databases
        dw_cursor.close()
        users_cursor.close()
        events_cursor.close()
        dojos_cursor.close()
        clean_databases(cursor, dojos, events, users)
        cursor.close
        sys.exit(0)
    except (psycopg2.Error) as e:
        print(e)
        sys.exit(1)

    if __name__ == '__main__':
        main()
