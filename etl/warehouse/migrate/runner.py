"""base class"""
from typing import Tuple, Type

from .migration import Migration


class Runner:
    """migration runner"""

    def __init__(self, select, insert) -> None:
        self._select = select
        self._insert = insert
        self._task, self._msg = self.setup()

    @staticmethod
    def setup() -> Tuple[Type[Migration], str]:
        """set up functiom to be overridden"""
        return Migration, ""

    async def run(self) -> None:
        """migration function"""
        self._select.execute(self._task.select_sql())
        self._insert.executemany(
            self._task.insert_sql(),
            [self._task(row).to_tuple() for row in self._select.fetchall()],
        )
        print(self._msg)
