import psycopg2
import psycopg2.extras

from .config import Config
from .migrate import migrate_db
from .warehouse import Store


async def migrate(config_path: str, dev: bool = False, db_path: str = "./db"):
    try:
        config = Config(config_path)
        store = Store(config.postgres, config.aws_s3, dev, db_path)
        await store.restore(config.databases)
        await migrate_db(config)
        print("Databases Migrated")
    except (psycopg2.Error) as err:
        print(err)
    finally:
        await store.close(
            config.databases.dojos, config.databases.events, config.databases.users
        )
        print(
            f"Removed {config.databases.dojos}, {config.databases.events} and {config.databases.users}"
        )
