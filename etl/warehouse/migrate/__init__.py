"""reads and writes to database"""

from psycopg2 import connect, cursor
from psycopg2.extras import DictCursor
from warehouse.local_types import Connection, Databases

from .badges import Badge, StagedBadge
from .dojos import Dojo, UserDojo
from .events import Event
from .leads import Lead
from .measures import Measure
from .staging import Stage
from .tickets import Ticket
from .users import User


class Migrator:
    """converts orginal dbs to warehouse"""

    def __init__(self, databases: Databases, con: Connection) -> None:
        self.dw_cursor = self._connect_db(databases.warehouse)
        self.dw_cursor.execute(open("./sql/dw.sql", "r").read())
        self.dojos_cursor = self._connect_db(databases.dojos)
        self.events_cursor = self._connect_db(databases.events)
        self.users_cursor = self._connect_db(databases.users)
        self.con = con
        self.databases = databases

    def _connect_db(self, database: str) -> cursor:
        conn = connect(
            dbname=database,
            host=self.con.host,
            user=self.con.user,
            password=self.con.password,
        )
        conn.set_session(autocommit=True)
        return conn.cursor(cursor_factory=DictCursor)

    async def disconnect(self) -> None:
        """disconnect from db's"""
        self.dw_cursor.connection.close()
        self.users_cursor.connection.close()
        self.events_cursor.connection.close()
        self.dojos_cursor.connection.close()

    async def migrate_db(self) -> None:
        """perform db migrations"""
        await self.__truncate()
        await self.__migrate_dojos()
        await self.__link_dojos_users()
        await self.__migrate_events()
        await self.__migrate_users()
        await self.__migrate_tickets()
        await self.__migrate_badges()
        await self.__migrate_leads()
        await self.__stage()
        await self.__stage_badges()
        await self.__measure()

    async def __truncate(self) -> None:
        """ Truncate all tables before fresh insert from sources"""
        self.dw_cursor.execute('TRUNCATE TABLE "factUsers" CASCADE')
        self.dw_cursor.execute('TRUNCATE TABLE "dimDojos" CASCADE')
        self.dw_cursor.execute('TRUNCATE TABLE "dimDojoLeads" CASCADE')
        self.dw_cursor.execute('TRUNCATE TABLE "dimUsers" CASCADE')
        self.dw_cursor.execute('TRUNCATE TABLE "dimEvents" CASCADE')
        self.dw_cursor.execute('TRUNCATE TABLE "dimLocation" CASCADE')
        self.dw_cursor.execute('TRUNCATE TABLE "dimUsersDojos" CASCADE')
        self.dw_cursor.execute('TRUNCATE TABLE "dimTickets" CASCADE')
        self.dw_cursor.execute('TRUNCATE TABLE "staging" CASCADE')
        self.dw_cursor.execute('TRUNCATE TABLE "dimBadges" CASCADE')

    async def __migrate_dojos(self) -> None:
        """Queries - Dojos"""
        self.dojos_cursor.execute(Dojo.select_sql())
        self.dw_cursor.executemany(
            Dojo.insert_sql(),
            [Dojo(row).to_tuple() for row in self.dojos_cursor.fetchall()],
        )
        print("Inserted all dojos")

    async def __link_dojos_users(self) -> None:
        """Queries - Dojos"""
        self.dojos_cursor.execute(UserDojo.select_sql())
        self.dw_cursor.executemany(
            UserDojo.insert_sql(),
            [link_users(row) for row in self.dojos_cursor.fetchall()],
        )
        print("Linked all dojos and users")

    async def __migrate_events(self) -> None:
        """Queries - Events"""
        self.events_cursor.execute(Event.select_sql())
        self.dw_cursor.executemany(
            Event.insert_sql(),
            [Event(row).to_tuple() for row in self.events_cursor.fetchall()],
        )
        print("Inserted all events and locations")

    async def __migrate_users(self) -> None:
        """ Queries - Users"""
        self.users_cursor.execute(User.select_sql())
        self.dw_cursor.executemany(
            User.insert_sql(),
            [User(row).to_tuple() for row in self.users_cursor.fetchall()],
        )
        print("Inserted all users")

    async def __migrate_tickets(self) -> None:
        """Queries - Tickets"""
        self.events_cursor.execute(Ticket.select_sql())
        self.dw_cursor.executemany(
            Ticket.insert_sql(),
            [Ticket(row).to_tuple() for row in self.events_cursor.fetchall()],
        )
        print("Inserted all tickets")

    async def __migrate_badges(self) -> None:
        """Queries - Badges"""
        self.users_cursor.execute(Badge.select_sql())
        for row in self.users_cursor.fetchall():
            self.dw_cursor.executemany(
                Badge.insert_sql(),
                [Badge(row["user_id"], badge).to_tuple() for badge in row["badges"]],
            )
        print("Inserted badges")

    async def __migrate_leads(self) -> None:
        """Queries - Leads"""
        self.dojos_cursor.execute(Lead.select_sql())
        self.dw_cursor.executemany(
            Lead.insert_sql(),
            [Lead(lead).to_tuple() for lead in self.dojos_cursor.fetchall()],
        )
        print("Inserted leads")

    async def __stage(self) -> None:
        """Queries - Staging"""
        self.events_cursor.execute(Stage.select_sql())
        self.dw_cursor.executemany(
            Stage.insert_sql(),
            [
                Stage(self.dw_cursor, row).to_tuple
                for row in self.events_cursor.fetchall()
            ],
        )
        print("Populated staging")

    async def __stage_badges(self) -> None:
        self.dw_cursor.execute(StagedBadge.select_sql())
        self.dw_cursor.executemany(
            StagedBadge.insert_sql(),
            [StagedBadge(row).to_tuple() for row in self.dw_cursor.fetchall()],
        )
        print("Badges added to staging")

    async def __measure(self) -> None:
        """Queries - Measures"""
        self.dw_cursor.execute(Measure.select_sql())
        self.dw_cursor.executemany(
            Measure.insert_sql(),
            [Measure(row).to_tuple() for row in self.dw_cursor.fetchall()],
        )
