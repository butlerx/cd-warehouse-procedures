"""Get all ids"""
from typing import Dict, Tuple
from uuid import uuid4


class Measure:
    """class for measurements"""

    def __init__(self, args: Dict) -> None:
        self.id: str = str(uuid4())
        self.dojo_id: str = args["dojo_id"]
        self.ticket_id: str = args["ticket_id"]
        self.event_id: str = args["event_id"]
        self.user_id: str = args["user_id"]
        self.time: str = args["time"]
        self.location_id: str = args["location_id"]
        self.badge_id: str = args["badge_id"]
        self.checked_in: str = args["checked_in"]

    def to_tuple(self) -> Tuple:
        """convert event to tuple"""
        return (
            self.dojo_id,
            self.ticket_id,
            self.event_id,
            self.user_id,
            self.time,
            self.location_id,
            self.id,
            self.badge_id,
            self.checked_in,
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
        ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
