""" restore from backups"""

from warehouse.localtypes import Connection

from .aws import AWS
from .restore import restore_db
from .s3 import download


async def download_db(
    con: Connection, aws: AWS, database: tuple, dev: bool = False
) -> None:
    """download if not in dev mode backups and restore from them"""
    print("Restoring db", database[0])
    if not dev:
        await download(database[0], aws, database[2])
    await restore_db(con, database)
