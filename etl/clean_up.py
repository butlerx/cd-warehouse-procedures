import psycopg2
import psycopg2.extras
from databases import cursor, dojos, dw, events, users


def reset_databases():
    try:
        drop_databases(users)
        drop_databases(dojos)
        drop_databases(events)
        drop_databases(dw)
        cursor.execute('DROP SCHEMA IF EXISTS "zen_source" CASCADE')
        cursor.execute('CREATE DATABASE "{0}"'.format(users))
        cursor.execute('CREATE DATABASE "{0}"'.format(dojos))
        cursor.execute('CREATE DATABASE "{0}"'.format(events))
        cursor.execute('CREATE DATABASE "{0}"'.format(dw))
    except (psycopg2.Error) as e:
        print(e)
        pass


def drop_databases(db):
    try:
        cursor.execute('''
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = "{0}"
              AND pid <> pg_backend_pid();
            '''.format(db))
        cursor.execute('DROP DATABASE IF EXISTS "{0}"'.format(db))
    except (psycopg2.Error) as e:
        print(e)
        pass


def clean_databases():
    drop_databases(users)
    drop_databases(dojos)
    drop_databases(events)
    drop_databases(dw)
