"""function related to events"""

from typing import Dict, Tuple

from .transform_json import get_city, get_country
from .migration import Migration


class Event(Migration):
    """event object"""

    def __init__(self, row: Dict) -> None:
        self._data = row
        self.id: str = row["id"]
        self.recurring_type: str = row["recurring_type"]
        self.created_at: str = row["created_at"]
        self.type: str = row["type"]
        self.dojo_id: str = row["dojo_id"]
        self.public: str = row["public"]
        self.status: str = row["status"]
        self.start_time: str = row["start_time"]

    @property
    def country(self) -> str:
        """country of event"""
        return get_country(self._data["country"])

    @property
    def is_eb(self) -> bool:
        """If the dojo is using eventbrite"""
        return self._data["eventbrite_id"] is not None

    @property
    def city(self) -> str:
        """city of event"""
        return get_city(self._data["city"])

    def to_tuple(self) -> Tuple:
        """convert event to tuple"""
        return (
            self.id,
            self.recurring_type,
            self.country,
            self.city,
            self.created_at,
            self.type,
            self.dojo_id,
            self.public,
            self.status,
            self.is_eb,
            self.start_time,
        )

    @staticmethod
    def select_sql() -> str:
        """sql for selecting events"""
        return """SELECT cd_events.*,
            CASE (d.date->>\'startTime\')
            WHEN \'Invalid date\'
                THEN NULL
                ELSE (d.date->>\'startTime\')::timestamp
            END start_time
            FROM cd_events
            LEFT OUTER JOIN (SELECT id, unnest(dates) as date
            FROM cd_events) d ON d.id = cd_events.id'"""

    @staticmethod
    def insert_sql() -> str:
        """sql for inserting events"""
        return """INSERT INTO "public"."dimEvents"(
            event_id,
            recurring_type,
            country,
            city,
            created_at,
            type,
            dojo_id,
            public,
            status,
            is_eb,
            start_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
