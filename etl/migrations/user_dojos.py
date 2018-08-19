"""for functions realted too dojos"""
from typing import Tuple, Type

from warehouse.migrate import Migration, Runner


def user_dojo(_) -> Type[Runner]:
    """link users and dojos"""
    return UserDojoMigration


class UserDojo(Migration):
    """link users too dojos"""

    def to_tuple(self) -> Tuple:
        """convert link to tuple"""
        return (
            self._data["id"],
            self._data["user_id"],
            self._data["dojo_id"],
            self._data["user_type"],
        )

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
