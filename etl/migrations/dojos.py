"""for functions realted too dojos"""
from typing import Tuple, Type

from warehouse.migrate import Migration, Runner

from .transform_json import get_city, get_country, get_county, get_state


def dojo(_) -> Type[Runner]:
    """migrate dojo data"""
    return DojoMigration


class Dojo(Migration):
    """dojos object"""

    @property
    def state(self) -> str:
        """dojos state"""
        return get_state(self._data["state"])

    @property
    def country(self) -> str:
        "dojos country" ""
        return get_country(self._data["country"])

    @property
    def county(self) -> str:
        "dojos county" ""
        return get_county(self._data["county"])

    @property
    def city(self) -> str:
        """dojos city"""
        return get_city(self._data["city"])

    @property
    def expected_attendees(self) -> int:
        """number of expected attendees"""
        return (
            self._data["expected_attendees"]
            if (self._data["expected_attendees"] is not None)
            else 0
        )  # Maybe something other than 0????

    @property
    def inactive(self) -> int:
        """a int to indicate if the dojo is inactive"""
        return 1 if (self._data["stage"] == 4) else 0

    @property
    def is_eb(self) -> int:
        """a int to indicate if the dojo is using eventbrite"""
        return (
            1
            if self._data["eventbrite_token"] and self._data["eventbrite_wh_id"]
            else 0
        )

    def to_tuple(self) -> Tuple:
        """convert dojo to tuple"""
        return (
            self._data["id"],
            self._data["created"],
            self._data["verified_at"],
            self._data["stage"],
            self.country,
            self.city,
            self.county,
            self.state,
            self._data["continent"],
            self._data["tao_verified"],
            self.expected_attendees,
            self._data["verified"],
            self._data["deleted"],
            self.inactive,
            self._data["inactive_at"],
            self.is_eb,
            self._data["dojo_lead_id"],
        )

    @staticmethod
    def insert_sql() -> str:
        """sql to insert dojo"""
        return """INSERT INTO "public"."dimDojos"(
            id,
            created,
            verified_at,
            stage,
            country,
            city,
            county,
            state,
            continent,
            tao_verified,
            expected_attendees,
            verified,
            deleted,
            inactive,
            inactive_at,
            is_eb,
            lead_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    @staticmethod
    def select_sql() -> str:
        """sql for selecting dojo"""
        return """SELECT * FROM cd_dojos
            LEFT JOIN(
                SELECT dojo_id, max(updated_at) as inactive_at
                FROM audit.dojo_stage
                WHERE stage=4 GROUP BY dojo_id)
            as q ON q.dojo_id = cd_dojos.id
            WHERE verified = 1 and deleted = 0"""


class DojoMigration(Runner):
    """dojo migration runner"""

    @staticmethod
    def setup() -> Tuple[Type[Dojo], str]:
        return Dojo, "Inserted all dojos"
