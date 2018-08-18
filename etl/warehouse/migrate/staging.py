"""function realted to staging"""
from datetime import datetime
from typing import Dict, Optional, Tuple
from uuid import uuid4

from isodate import parse_datetime
from psycopg2 import cursor

from .transform_json import get_city, get_country


class Stage:
    """class responsible for staging data"""

    def __init__(self, db_cursor: cursor, row: Dict) -> None:
        self._data = row
        self.cursor: cursor = db_cursor
        self.location_id = str(uuid4())
        self._insert()
        self.id: str = row["id"]
        self.user_id: str = row["user_id"]
        self.dojo_id: str = row["dojo_id"]
        self.event_id: str = row["event_id"]
        self.session_id: str = row["session_id"]
        self.ticket_id: str = row["ticket_id"]

    @property
    def attendance(self) -> bool:
        """does user attend """
        return False if not self._data["attendance"] else True

    @property
    def time(self) -> Optional[datetime]:
        """start time"""
        return (
            parse_datetime(self._data["dates"][0]["startTime"])
            if self._data["dates"][0]["startTime"] is not None
            else None
        )

    def to_tuple(self) -> Tuple:
        """convert staged info to tuple"""
        return (
            self.user_id,
            self.dojo_id,
            self.event_id,
            self.session_id,
            self.ticket_id,
            self.attendance,
            self.time,
            self.location_id,
            self.id,
        )

    @property
    def country(self) -> str:
        """get country"""
        return get_country(self._data["country"])

    @property
    def city(self) -> str:
        """get city"""
        return get_city(self._data["city"])

    def _insert(self) -> None:
        self.cursor.execute(
            """
            INSERT INTO "public"."dimLocation"(
                country,
                city,
                location_id
            ) VALUES (%s, %s, %s)
        """,
            (self.country, self.city, self.location_id),
        )

    @staticmethod
    def select_sql() -> str:
        """sql for selecting staging data"""
        return """SELECT cd_applications.id, cd_applications.ticket_id,
                cd_applications.session_id, cd_applications.event_id,
                cd_applications.dojo_id, cd_applications.user_id,
                cd_applications.attendance,
                dates, country, city
            FROM cd_applications
            INNER JOIN cd_events ON cd_applications.event_id = cd_events.id"""

    @staticmethod
    def insert_sql() -> str:
        """sql for inserting staged info"""
        return """INSERT INTO "staging"(
            user_id,
            dojo_id,
            event_id,
            session_id,
            ticket_id,
            checked_in,
            time,
            location_id,
            id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
