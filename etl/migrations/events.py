"""function related to events"""
from typing import Tuple, Type

from warehouse.migrate import Migration, Runner

from .transform_json import get_city, get_country


def event(_) -> Type[Runner]:
    """event Migration"""
    return _EventMigration


class Event(Migration):
    """event object"""

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
            self._data["id"],
            self._data["recurring_type"],
            self.country,
            self.city,
            self._data["created_at"],
            self._data["type"],
            self._data["dojo_id"],
            self._data["public"],
            self._data["status"],
            self.is_eb,
            self._data["start_time"],
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


class _EventMigration(Runner):
    @staticmethod
    def setup() -> Tuple[Type[Event], str]:
        return Event, "Inserted all events and locations"
