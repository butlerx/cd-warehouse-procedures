"""function realted to staging"""
from typing import Dict, Tuple
from uuid import uuid4

from isodate import parse_datetime

from .transform_json import get_city, get_country


def stage(cursor):
    """returns function for staging data"""

    def _calculate(row: Dict) -> Tuple:
        attendance = False if not row["attendance"] else True
        time = (
            parse_datetime(row["dates"][0]["startTime"])
            if row["dates"][0]["startTime"] is not None
            else None
        )
        location_id = str(uuid4())
        cursor.execute(
            """
            INSERT INTO "public"."dimLocation"(
                country,
                city,
                location_id
            ) VALUES (%s, %s, %s)
        """,
            (get_country(row["country"]), get_city(row["city"]), location_id),
        )
        return (
            row["user_id"],
            row["dojo_id"],
            row["event_id"],
            row["session_id"],
            row["ticket_id"],
            attendance,
            time,
            location_id,
            row["id"],
        )

    return _calculate
