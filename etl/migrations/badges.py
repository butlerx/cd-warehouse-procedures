"""Badge related transformations"""
from typing import Dict, Tuple, Type
from uuid import uuid4

from warehouse import Migration, Runner


def staged_badge(_) -> Type[Runner]:
    """staged badge function"""

    class StagedBadge(Migration):
        """staged badge"""

        def __init__(self, row: Dict) -> None:
            self.badge_id: str = row["badge_id"]
            self.user_id: str = row["user_id"]

        def to_tuple(self) -> Tuple:
            """convert to tuple"""
            return (self.badge_id, self.user_id)

        @staticmethod
        def select_sql() -> str:
            """sql to select badge"""
            return 'SELECT badge_id, user_id FROM "dimBadges"'

        @staticmethod
        def insert_sql() -> str:
            """sql to insert badge"""
            return 'UPDATE "staging" SET badge_id=%s WHERE user_id=%s'

    class StagedBadgeMigration(Runner):
        """StagedBadge migrations"""

        @staticmethod
        def setup() -> Tuple[Type[StagedBadge], str]:
            return StagedBadge, "Badges added to staging"

    return StagedBadgeMigration


def badge(_) -> Type[Runner]:
    """badge bootstap"""

    class Badge(Migration):
        """badge object"""

        def __init__(self, user_id: str, row: Dict) -> None:
            self._data = row
            self.user_id = user_id
            self.badge_id = str(uuid4())

        def to_tuple(self) -> Tuple:
            """convert badge to tuple"""
            return (
                self.id,
                self.archived,
                self.type,
                self.name,
                self.badge_id,
                self.user_id,
                self.issued_on,
            )

        @property
        def issued_on(self) -> str:
            """when badge was issued"""
            return self._data.get("assertion", {}).get("issuedOn", None)

        @property
        def id(self) -> str:
            """Original id of badge"""
            return self._data["id"]

        @property
        def archived(self) -> bool:
            """if the badge is archived"""
            return self._data["archived"]

        @property
        def type(self) -> str:
            """badge type"""
            return self._data["type"]

        @property
        def name(self) -> str:
            """name of badge"""
            return self._data["name"]

        @staticmethod
        def select_sql() -> str:
            """sql to select badge"""
            return """SELECT user_id, to_json(badges)
                AS badges
                FROM cd_profiles
                WHERE badges IS NOT null
                AND json_array_length(to_json(badges)) >= 1"""

        @staticmethod
        def insert_sql() -> str:
            """sql to insert badge"""
            return """INSERT INTO "public"."dimBadges"(
                id,
                archived,
                type,
                name,
                badge_id,
                user_id,
                issued_on)
            VALUES (%s, %s, %s, %s, %s, %s, %s)"""

    class BadgeMigration(Runner):
        """Badge migrations"""

        @staticmethod
        def setup() -> Tuple[Type[Badge], str]:
            """set up functiom to be overridden"""
            return Badge, "Inserted badges"

        async def run(self) -> None:
            """Queries - Badges"""
            self._select.execute(self._task.select_sql())
            for row in self._select.fetchall():
                self._insert.executemany(
                    self._task.insert_sql(),
                    [
                        self._task(row["user_id"], badge).to_tuple()
                        for badge in self._select.fetchall()
                    ],
                )
            print(self._msg)

    return BadgeMigration
