"""reads and writes to database"""
from typing import List, Tuple

from psycopg2 import connect, cursor
from psycopg2.extras import DictCursor
from warehouse.local_types import Connection, Databases


class Migrator:
    """converts orginal dbs to warehouse"""

    def __init__(self, databases: Databases, con: Connection) -> None:
        self.dw_cursor = self._connect_db(con, databases.warehouse)
        self.dw_cursor.execute(open("./sql/dw.sql", "r").read())
        self.cursors = {
            "dw": self.dw_cursor,
            "dojos": self._connect_db(con, databases.dojos),
            "events": self._connect_db(con, databases.events),
            "users": self._connect_db(con, databases.users),
        }

    @staticmethod
    def _connect_db(con: Connection, database: str) -> cursor:
        return (
            connect(
                dbname=database, host=con.host, user=con.user, password=con.password
            )
            .set_session(autocommit=True)
            .cursor(cursor_factory=DictCursor)
        )

    async def _disconnect(self) -> None:
        """disconnect from db's"""
        for _, curs in self.cursors.items():
            curs.connection.close()

    async def migrate_db(self, tables: List[str], tasks: List[Tuple]) -> None:
        """perform db migrations"""
        await self._truncate(tables)
        await self._tasks(tasks)
        await self._disconnect()

    async def _truncate(self, tables: List[str]) -> None:
        """ Truncate all tables before fresh insert from sources"""
        self.dw_cursor.execute(
            ";".join(['TRUNCATE TABLE "{}" CASCADE'.format(table) for table in tables])
        )

    async def _tasks(self, tasks: List[Tuple]) -> None:
        """Runs Migration"""
        for db, task in tasks:
            await task(self.cursors["dw"])(self.cursors[db], self.cursors["dw"]).run()
