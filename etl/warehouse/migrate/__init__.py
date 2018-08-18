"""reads and writes to database"""
from typing import List, Tuple

from psycopg2 import connect, cursor
from psycopg2.extras import DictCursor
from warehouse.local_types import Connection, Databases

from .badges import Badge, StagedBadge
from .dojos import Dojo, UserDojo
from .events import Event
from .leads import Lead
from .measures import Measure
from .staging import staging
from .tickets import Ticket
from .users import User


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

    async def migrate_db(self) -> None:
        """perform db migrations"""
        await self._truncate()
        await self._tasks(
            [
                ("dojos", Dojo, "Inserted all dojos"),
                ("dojos", UserDojo, "Linked all dojos and users"),
                ("events", Event, "Inserted all events and locations"),
                ("users", User, "Inserted all users"),
                ("events", Ticket, "Inserted all tickets"),
            ]
        )
        await self._migrate_badges()
        await self._tasks(
            [
                ("dojos", Lead, "Inserted leads"),
                ("events", staging(self.dw_cursor), "Populated staging"),
                ("dw", StagedBadge, "Badges added to staging"),
                ("dw", Measure, ""),
            ]
        )
        await self._disconnect()

    async def _truncate(self) -> None:
        """ Truncate all tables before fresh insert from sources"""
        self.dw_cursor.execute(
            ";".join(
                [
                    'TRUNCATE TABLE "{}" CASCADE'.format(table)
                    for table in [
                        "factUsers",
                        "dimDojos",
                        "dimDojoLeads",
                        "dimUsers",
                        "dimEvents",
                        "dimLocation",
                        "dimUsersDojos",
                        "dimTickets",
                        "staging",
                        "dimBadges",
                    ]
                ]
            )
        )

    async def _tasks(self, tasks: List[Tuple]) -> None:
        """Runs Migration"""
        for db, Task, msg in tasks:
            self.cursors[db].execute(Task.select_sql())
            self.dw_cursor.executemany(
                Task.insert_sql(),
                [Task(row).to_tuple() for row in self.cursors[db].fetchall()],
            )
            print(msg)

    async def _migrate_badges(self) -> None:
        """Queries - Badges"""
        self.cursors["users"].execute(Badge.select_sql())
        for row in self.cursors["users"].fetchall():
            self.dw_cursor.executemany(
                Badge.insert_sql(),
                [Badge(row["user_id"], badge).to_tuple() for badge in row["badges"]],
            )
        print("Inserted badges")
