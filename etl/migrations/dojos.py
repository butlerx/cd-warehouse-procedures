"""for functions realted too dojos"""
from typing import Dict, Tuple, Type

from warehouse import Migration, Runner

from .transform_json import get_city, get_country, get_county, get_state


class UserDojo(Migration):
    """link users too dojos"""

    def __init__(self, row: Dict) -> None:
        self.id: str = row["id"]
        self.user_id: str = row["user_id"]
        self.dojo_id: str = row["dojo_id"]
        self.user_type: str = row["user_type"]

    def to_tuple(self) -> Tuple:
        """convert link to tuple"""
        return (self.id, self.user_id, self.dojo_id, self.user_type)

    @staticmethod
    def insert_sql() -> str:
        """sql to insert link"""
        return """INSERT INTO "public"."dimUsersDojos"(
                id,
                user_id,
                dojo_id,
                user_type)
            VALUES (%s, %s, %s, %s)"""

    @staticmethod
    def select_sql() -> str:
        """sql for selecting dojo"""
        return """SELECT
                id,
                user_id,
                dojo_id,
                unnest(user_types) as user_type
            FROM cd_usersdojos
            WHERE deleted = 0"""


class UserDojoMigration(Runner):
    """user dojo migration runner"""

    @staticmethod
    def setup() -> Tuple[Type[UserDojo], str]:
        return UserDojo, "Linked all dojos and users"


class Dojo(Migration):
    """dojos object"""

    def __init__(self, row: Dict) -> None:
        self._data = row
        self.id: str = row["id"]
        self.created: str = row["created"]
        self.verified_at: str = row["verified_at"]
        self.stage: int = row["stage"]
        self.verified: bool = row["verified"]
        self.deleted: bool = row["deleted"]
        self.inactive_at: str = row["inactive_at"]
        self.dojo_lead_id: str = row["dojo_lead_id"]
        self.continent: str = row["continent"]
        self.tao_verified: bool = row["tao_verified"]

    @property
    def country(self) -> str:
        """country dojo is in"""
        return get_country(self._data["country"])

    @property
    def county(self) -> str:
        """county the dojo is in"""
        return get_county(self._data["county"])

    @property
    def city(self) -> str:
        """dojos city"""
        return get_city(self._data["city"])

    @property
    def state(self) -> str:
        """dojos state"""
        return get_state(self._data["state"])

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
        # Why a int not a bool
        return 1 if (self._data["stage"] == 4) else 0

    @property
    def is_eb(self) -> int:
        """a int to indicate if the dojo is using eventbrite"""
        # Why a int not a bool
        return (
            1
            if self._data["eventbrite_token"] and self._data["eventbrite_wh_id"]
            else 0
        )

    def to_tuple(self) -> Tuple:
        """convert dojo to tuple"""
        return (
            self.id,
            self.created,
            self.verified_at,
            self.stage,
            self.country,
            self.city,
            self.county,
            self.state,
            self.continent,
            self.tao_verified,
            self.expected_attendees,
            self.verified,
            self.deleted,
            self.inactive,
            self.inactive_at,
            self.is_eb,
            self.dojo_lead_id,
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
        VALUES ( % s, % s, % s, % s, % s, % s, % s, % s, % s, % s, % s, % s, % s, % s, % s, % s, % s)"""

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
