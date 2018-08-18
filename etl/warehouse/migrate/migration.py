"""base class"""
from typing import Tuple


class Migration:
    """base migration class"""

    @staticmethod
    def insert_sql() -> str:
        """command to insert"""
        pass

    @staticmethod
    def select_sql() -> str:
        """command to select"""
        pass

    def to_tuple(self) -> Tuple:
        """convert migration to tuple"""
        pass