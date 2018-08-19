"""Staged Badge related transformations"""
from typing import Tuple, Type

from warehouse.migrate import Migration, Runner


def staged_badge(_) -> Type[Runner]:
    """staged badge function"""
    return StagedBadgeMigration


class StagedBadge(Migration):
    """staged badge"""

    def to_tuple(self) -> Tuple:
        """convert to tuple"""
        return (self._data["badge_id"], self._data["user_id"])

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
