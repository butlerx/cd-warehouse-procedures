import json

import psycopg2
import psycopg2.extras
from clean_up import reset_databases

with open('./config/config.json') as data:
    data = json.load(data)

db_host = data['postgres']['host']
db_password = data['postgres']['password']
db_user = data['postgres']['user']
dojos = data['databases']['dojos']
dw = data['databases']['dw']
events = data['databases']['events']
users = data['databases']['users']

dw_setup = psycopg2.connect(
    dbname='postgres', host=db_host, user=db_user, password=db_password)
dw_setup.set_session(autocommit=True)
cursor = dw_setup.cursor()
reset_databases(cursor, dojos, dw, events, users)

dw_conn = psycopg2.connect(
    dbname=dw, host=db_host, user=db_user, password=db_password)
dw_cursor = dw_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
dojos_conn = psycopg2.connect(
    dbname=dojos, host=db_host, user=db_user, password=db_password)
dojos_cursor = dojos_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
events_conn = psycopg2.connect(
    dbname=events, host=db_host, user=db_user, password=db_password)
events_cursor = events_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
users_conn = psycopg2.connect(
    dbname=users, host=db_host, user=db_user, password=db_password)
users_cursor = users_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
