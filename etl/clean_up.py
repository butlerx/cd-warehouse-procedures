import psycopg2
import psycopg2.extras


def reset_databases(cursor, dojos, dw, events, users):
    try:
        clean_databases(cursor, dojos, events, users)
        drop_databases(cursor, dw)
        cursor.execute('CREATE DATABASE "{0}";'.format(users))
        cursor.execute('CREATE DATABASE "{0}"'.format(dojos))
        cursor.execute('CREATE DATABASE "{0}"'.format(events))
        cursor.execute('CREATE DATABASE "{0}"'.format(dw))
    except (psycopg2.Error) as e:
        print(e)


def drop_databases(cursor, db):
    try:
        disconnect(cursor, db)
        cursor.execute('DROP DATABASE IF EXISTS "{0}"'.format(db))
    except (psycopg2.Error) as e:
        print(e)


def clean_databases(cursor, dojos, events, users):
    drop_databases(cursor, users)
    drop_databases(cursor, dojos)
    drop_databases(cursor, events)


def disconnect(cursor, db):
    try:
        cursor.execute('''
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = "{0}"
              AND pid <> pg_backend_pid();
            '''.format(db))
    except (psycopg2.Error):
        pass
