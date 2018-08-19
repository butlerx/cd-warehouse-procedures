"""function realted to staging"""
from datetime import datetime
from typing import Dict, Optional, Tuple, Type
from uuid import uuid4

from isodate import parse_datetime
from psycopg2 import cursor

from .migration import Migration, Runner
from .transform_json import get_city, get_country


def staging(db_cursor: cursor) -> Type[Runner]:
    """return stage class"""

    class Stage(Migration):
        """class responsible for staging data"""

        def __init__(self, row: Dict) -> None:
            super().__init__(row)
            self.cursor: cursor = db_cursor
            self.location_id = str(uuid4())
            self._insert()

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
                self._data["user_id"],
                self._data["dojo_id"],
                self._data["event_id"],
                self._data["session_id"],
                self._data["ticket_id"],
                self.attendance,
                self.time,
                self.location_id,
                self._data["id"],
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

    class _StageMigration(Runner):
        @staticmethod
        def setup() -> Tuple[Type[Stage], str]:
            """Migrate Staged data"""
            return Stage, "Populated staging"

    return _StageMigration
