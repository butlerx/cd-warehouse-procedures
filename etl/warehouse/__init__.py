"""warehouse main interface"""
from sys import exit as _exit
from typing import Dict

from .data_warehouse import AWS, Store
from .local_types import Connection, Databases
from .migrate import migrate_db


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
    store = Store(
        con,
        AWS(
            bucket=config["s3"]["bucket"],
            access_key=config["s3"]["access"],
            secret_key=config["s3"]["secret"],
        ),
        dev,
        db_path,
    )
    try:
        await store.restore(databases)
        await migrate_db(databases, con, config["tables"], config["migrations"])
        print("Databases Migrated")
        exit_code = 0
    except Exception as err:
        print(err)
        exit_code = 1
    finally:
        await store.close(databases.dojos, databases.events, databases.users)
        print(
            "Removed {0}, {1} and {2}".format(
                databases.dojos, databases.events, databases.users
            )
        )
        _exit(exit_code)
