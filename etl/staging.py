"""function realted to staging"""
import uuid
from typing import Dict, Tuple

import psycopg2
import psycopg2.extras
from isodate import parse_datetime


def stage(cursor):
    """returns function for staging data"""

    def _calculate(row: Dict) -> Tuple:
        attendance = False if not row['attendance'] else True
        time = parse_datetime(row['dates'][0]['startTime']) if row['dates'][0][
            'startTime'] is not None else None
        location_id = insert_location(row['city'], row['country'])
        return (row['user_id'], row['dojo_id'], row['event_id'],
                row['session_id'], row['ticket_id'], attendance, time,
                location_id, row['id'])

    def insert_location(city: Dict, country: Dict) -> str:
        """insert location in to db"""
        location_id = str(uuid.uuid4())

        # For fields which zen prod dbs are storing as json
        country = country if (country is not None) and country else None
        country = country['countryName'] if country is not None else 'Unknown'

        city = city if (city is not None) and city else None
        city = 'Unknown' if city is None else city[
            'toponymName'] if 'toponymName' in city else city[
                'nameWithHierarchy']

        cursor.execute('''
            INSERT INTO "public"."dimLocation"(
                country,
                city,
                location_id
            ) VALUES (%s, %s, %s)
        ''', (country, city, location_id))
        return location_id

    return _calculate
