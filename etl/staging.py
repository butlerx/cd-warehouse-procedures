import uuid

import psycopg2
import psycopg2.extras
from isodate import parse_datetime


def stage(cursor):
    def calculate(row):
        user_id = row['user_id']
        dojo_id = row['dojo_id']
        event_id = row['event_id']
        session_id = row['session_id']
        ticket_id = row['ticket_id']
        checked_in = False if row['attendances'] is not else True
        time = None
        location_id = str(uuid.uuid4())
        id = row['id']
        start_date = row['dates']

        if start_date[0]['startTime'] is not None:
            start_date = parse_datetime(start_date[0]['startTime'])
            time = start_date

        country = row['country'] if (row['country'] is not None) and (
            len(row['country'])) > 0 else 'Unknown'
        city = row['city'] if (
            row['city'] is not None) and (len(row['city'])) > 0 else 'Unknown'

        # For fields which zen prod dbs are storing as json
        if country is not 'Unknown':
            country = country['countryName']

        if city is not 'Unknown':
            if 'toponymName' in city:
                city = city['toponymName']
            else:
                city = city['nameWithHierarchy']

        insert_location(cursor, country, city, location_id)

        data = (user_id, dojo_id, event_id, session_id, ticket_id, time,
                location_id, id)
        return data

    return calculate


def insert_location(cursor, country, city, location_id):
    sql = '''
        INSERT INTO "public"."dimLocation"(
            country,
            city,
            location_id
        ) VALUES (%s, %s, %s)
    '''
    data = (country, city, location_id)
    try:
        cursor.execute(sql, data)
    except (psycopg2.Error) as e:
        raise (e)
