from asyncio import gather

from psycopg2 import Error, connect
from psycopg2.extras import DictCursor

from .config import Config, Postgres
from .migrations import (
    add_badges,
    add_ids,
    link_users,
    migrate_badges,
    migrate_dojos,
    migrate_events,
    migrate_lead,
    migrate_tickets,
    migrate_users,
    populate_staging,
)


async def setup_warehouse(dw_cursor):
    try:
        dw_cursor.execute(open("./sql/dw.sql", "r").read())
    except Error as err:
        print(err)


async def migrate_db(config: Config) -> None:
    """converts orginal dbs to warehouse"""
    dw_cursor = _connect_db(config.postgres, config.databases.warehouse)
    await setup_warehouse(dw_cursor)
    cursors = {
        "dw": dw_cursor,
        "dojos": _connect_db(config.postgres, config.databases.dojos),
        "events": _connect_db(config.postgres, config.databases.events),
        "users": _connect_db(config.postgres, config.databases.users),
    }
    dw_cursor.execute(
        """
        TRUNCATE TABLE "factUsers" CASCADE;
        TRUNCATE TABLE "dimDojos" CASCADE;
        TRUNCATE TABLE "dimDojoLeads" CASCADE;
        TRUNCATE TABLE "dimUsers" CASCADE;
        TRUNCATE TABLE "dimEvents" CASCADE;
        TRUNCATE TABLE "dimLocation" CASCADE;
        TRUNCATE TABLE "dimUsersDojos" CASCADE;
        TRUNCATE TABLE "dimTickets" CASCADE;
        TRUNCATE TABLE "staging" CASCADE;
        TRUNCATE TABLE "dimBadges" CASCADE;
        """
    )
    await gather(
        migrate_dojos(cursors["dojos"], dw_cursor),
        link_users(cursors["dojos"], dw_cursor),
        migrate_events(cursors["events"], dw_cursor),
        migrate_users(cursors["users"], dw_cursor),
        migrate_tickets(cursors["events"], dw_cursor),
        migrate_badges(cursors["users"], dw_cursor),
        migrate_lead(cursors["dojos"], dw_cursor),
        populate_staging(cursors["events"], dw_cursor),
    )
    await add_badges(dw_cursor)
    await add_ids(dw_cursor)
    for _, curs in cursors.items():
        curs.connection.close()


def _connect_db(con: Postgres, database: str):
    connection = connect(
        dbname=database, host=con.host, user=con.user, password=con.password
    )
    connection.set_session(autocommit=True)
    return connection.cursor(cursor_factory=DictCursor)
