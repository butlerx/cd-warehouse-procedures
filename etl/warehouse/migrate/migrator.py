"""reads and writes to database"""
from typing import List, Tuple

from psycopg2 import connect
from psycopg2.extras import DictCursor
from warehouse.local_types import Connection, Databases


async def migrate_db(
    databases: Databases, con: Connection, tables: List[str], tasks: List[Tuple]
) -> None:
    """converts orginal dbs to warehouse"""
    dw_cursor = _connect_db(con, databases.warehouse)
    dw_cursor.execute(open("./sql/dw.sql", "r").read())
    cursors = {
        "dw": dw_cursor,
        "dojos": _connect_db(con, databases.dojos),
        "events": _connect_db(con, databases.events),
        "users": _connect_db(con, databases.users),
    }
    dw_cursor.execute(
        ";".join(['TRUNCATE TABLE "{}" CASCADE'.format(table) for table in tables])
    )
    for database, task in tasks:
        await task(cursors["dw"])(cursors[database], cursors["dw"]).run()
    for _, curs in cursors.items():
        curs.connection.close()


def _connect_db(con: Connection, database: str):
    connection = connect(
        dbname=database, host=con.host, user=con.user, password=con.password
    )
    connection.set_session(autocommit=True)
    return connection.cursor(cursor_factory=DictCursor)
