from asyncio import gather
from uuid import uuid4

from isodate import parse_datetime
from psycopg2 import Error


async def stage(cursor, row: dict) -> tuple:
    location_id = str(uuid4())

    city = row["city"] if (row["city"] is not None) and row["city"] else "Unknown"
    if city != "Unknown":
        city = (
            city["toponymName"] if "toponymName" in city else city["nameWithHierarchy"]
        )

    insert_location(
        cursor,
        (
            row["country"]["countryName"]
            if (row["country"] is not None)
            and row["country"]
            and "countryName" in row["country"]
            else "Unknown"
        ),
        city,
        location_id,
    )

    return (
        row["user_id"],
        row["dojo_id"],
        row["event_id"],
        row["session_id"],
        row["ticket_id"],
        False if not row["attendance"] else True,
        (
            parse_datetime(row["dates"][0]["startTime"])
            if row["dates"][0]["startTime"] is not None
            else None
        ),
        location_id,
        row["id"],
    )


async def insert_location(cursor, country, city, location_id):
    cursor.execute(
        """INSERT INTO "public"."dimLocation"(
            country,
            city,
            location_id
        ) VALUES (%s, %s, %s)""",
        (country, city, location_id),
    )


async def populate_staging(cursor, dw_cursor):
    cursor.execute(
        """SELECT cd_applications.id, cd_applications.ticket_id,
            cd_applications.session_id, cd_applications.event_id,
            cd_applications.dojo_id, cd_applications.user_id,
            cd_applications.attendance,
            dates, country, city
        FROM cd_applications
        INNER JOIN cd_events ON cd_applications.event_id = cd_events.id"""
    )
    dw_cursor.executemany(
        """INSERT INTO "staging"(
            user_id,
            dojo_id,
            event_id,
            session_id,
            ticket_id,
            checked_in,
            time,
            location_id,
            id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        await gather(*(stage(dw_cursor, event) for event in cursor.fetchall())),
    )
    print("Populated staging")
