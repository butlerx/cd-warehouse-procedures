"""Get all ids"""
from typing import Dict, Tuple, Type
from uuid import uuid4

from warehouse.migrate import Migration, Runner


def measure(_):
    """Measurement migrations"""
    return _MeasureMigration


class Measure(Migration):
    """class for measurements"""

    def __init__(self, args: Dict) -> None:
        super().__init__(args)
        self.id: str = str(uuid4())

    def to_tuple(self) -> Tuple:
        """convert event to tuple"""
        return (
            self._data["dojo_id"],
            self._data["ticket_id"],
            self._data["event_id"],
            self._data["user_id"],
            self._data["time"],
            self._data["location_id"],
            self.id,
            self._data["badge_id"],
            self._data["checked_in"],
        )

    @staticmethod
    def select_sql() -> str:
        """sql for selecting events"""
        return """SELECT "staging".dojo_id, "staging".ticket_id,
            "staging".session_id, "staging".event_id,
            "staging".user_id, "staging".time, "staging".location_id,
            "staging".badge_id, "staging".checked_in
        FROM "staging"
        INNER JOIN "dimDojos"
          ON "staging".dojo_id = "dimDojos".id
        INNER JOIN "dimUsers"
          ON "staging".user_id = "dimUsers".user_id
        INNER JOIN "dimLocation" ON
          "staging".location_id = "dimLocation".location_id
        INNER JOIN "dimBadges"
          ON "staging".badge_id = "dimBadges".badge_id
        GROUP BY "staging".event_id, "staging".dojo_id,
            "staging".ticket_id, "staging".session_id, "staging".event_id,
            "staging".user_id, "staging".time, "staging".location_id,
            "staging".badge_id, "staging".checked_in"""

    @staticmethod
    def insert_sql() -> str:
        """sql for inserting"""
        return """INSERT INTO "public"."factUsers"(
            dojo_id,
            ticket_id,
            event_id,
            user_id,
            time,
            location_id,
            id,
            badge_id,
            checked_in
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""


class _MeasureMigration(Runner):
    """migration of measurements"""

    @staticmethod
    def setup() -> Tuple[Type[Measure], str]:
        return Measure, ""
