"""warehouse main interface"""
from asyncio import gather
from sys import exit as _exit
from typing import Dict

from psycopg2 import Error

from .clean_up import Cleaner
from .local_types import AWS, Connection, Databases
from .migrate import Migrator
from .restore import restore_db
from .s3 import download


class Warehouse:
    """main class for parseing config and running migration"""

    def __init__(self, config: Dict) -> None:
        self.con = Connection(
            host=config["postgres"]["host"],
            password=config["postgres"]["password"],
            user=config["postgres"]["user"],
        )
        self.databases = Databases(
            warehouse=config["databases"]["dw"],
            dojos=config["databases"]["dojos"],
            events=config["databases"]["events"],
            users=config["databases"]["users"],
        )
        self.aws = AWS(
            bucket=config["s3"]["bucket"],
            access_key=config["s3"]["access"],
            secret_key=config["s3"]["secret"],
        )
        self.cleaner = Cleaner(self.con)

    async def download(
        self, name: str, database: str, path: str, dev: bool = False
    ) -> None:
        """download if not in dev mode backups and restore from them"""
        print("Restoring db", name)
        if not dev:
            await download(name, self.aws, path)
        await restore_db(self.con, database, path, name)

    async def run(self, dev: bool = False, db_path: str = "./db") -> None:
        """main function"""
        try:
            await self.cleaner.reset_databases(self.databases)
            print("Databases Reset")

            await gather(
                self.download("dojos", self.databases.dojos, db_path, dev),
                self.download("events", self.databases.events, db_path, dev),
                self.download("users", self.databases.users, db_path, dev),
            )

            convertor = Migrator(self.databases, self.con)
            await convertor.migrate_db()
            print("Databases Migrated")
            await convertor.disconnect()
            self._exit(0)
        except Error as err:
            print(err)
            self._exit(1)

    def _exit(self, exit_code: int):
        self.cleaner.close(
            self.databases.dojos, self.databases.events, self.databases.users
        )
        print(
            "Removed {0}, {1} and {2}".format(
                self.databases.dojos, self.databases.events, self.databases.users
            )
        )
        _exit(exit_code)