"""for functions realted too users"""
from typing import Dict, Tuple, Type

from warehouse import Migration, Runner

from .transform_json import get_city, get_country


def user(_) -> Type[Runner]:
    """user migrations"""

    class User(Migration):
        """user object"""

        @property
        def country(self) -> str:
            """users country"""
            return get_country(self._data["country"])

        @property
        def city(self) -> str:
            """"users city"""
            return get_city(self._data["city"])

        @property
        def gender(self) -> str:
            """users gender"""
            return (
                self._data["gender"]
                if (self._data["gender"] is not None) and self._data["gender"]
                else "Unknown"
            )

        @property
        def role(self) -> str:
            """users role"""
            return self._data["roles"][0] if self._data["roles"] else "Unknown"

        def to_tuple(self) -> Tuple:
            """Transform to Tuple"""
            return (
                self._data["user_id"],
                self._data["dob"],
                self.country,
                self.city,
                self.gender,
                self._data["user_type"],
                self.role,
                self._data["mailing_list"],
                self._data["when"],
            )

        @staticmethod
        def insert_sql() -> str:
            """sql command to insert user"""
            return """INSERT INTO "public"."dimUsers"(
                user_id,
                dob,
                country,
                city,
                gender,
                user_type,
                roles,
                mailing_list,
                created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        @staticmethod
        def select_sql() -> str:
            """sql command to select user"""
            return """SELECT *
                FROM cd_profiles
                INNER JOIN sys_user ON cd_profiles.user_id = sys_user.id"""

    class _UserMigration(Runner):
        @staticmethod
        def setup() -> Tuple[Type[User], str]:
            """set type and message"""
            return User, "Inserted all users"

    return _UserMigration
