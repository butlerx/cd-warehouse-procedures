"""warehouse main interface"""
from asyncio import gather
from sys import exit as _exit
from typing import Dict

from .backups import AWS, download_db
from .clean_up import Cleaner
from .local_types import Connection, Databases
from .migrate import Migration, Migrator, Runner


async def warehouse(config: Dict, dev: bool = False, db_path: str = "./db") -> None:
    """main class for parseing config and running migration"""
    con = Connection(
        host=config["postgres"]["host"],
        password=config["postgres"]["password"],
        user=config["postgres"]["user"],
    )
    databases = Databases(
        warehouse=config["databases"]["dw"],
        dojos=config["databases"]["dojos"],
        events=config["databases"]["events"],
        users=config["databases"]["users"],
    )
    aws = AWS(
        bucket=config["s3"]["bucket"],
        access_key=config["s3"]["access"],
        secret_key=config["s3"]["secret"],
    )
    cleaner = Cleaner(con)
    try:
        await cleaner.reset_databases(databases)
        print("Databases Reset")
        await gather(
            download_db(con, aws, ("dojos", databases.dojos, db_path), dev),
            download_db(con, aws, ("events", databases.events, db_path), dev),
            download_db(con, aws, ("users", databases.users, db_path), dev),
        )
        await Migrator(databases, con).migrate_db(
            config["tables"], config["migrations"]
        )
        print("Databases Migrated")
        exit_code = 0
    except Exception as err:
        print(err)
        exit_code = 1
    finally:
        await cleaner.close(databases.dojos, databases.events, databases.users)
        print(
            "Removed {0}, {1} and {2}".format(
                databases.dojos, databases.events, databases.users
            )
        )
        _exit(exit_code)
