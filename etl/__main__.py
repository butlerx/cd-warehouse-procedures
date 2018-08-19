#! /usr/bin/env python3
"""main function"""

from argparse import ArgumentParser
from asyncio import get_event_loop
from json import load

from migrations import MIGRATIONS
from warehouse import warehouse


def cli():
    """cli interface"""
    parser = ArgumentParser(
        prog="etl", description="migrate production databases backups to datawarehouse"
    )
    parser.add_argument(
        "--dev", action="store_true", help="dev mode to use local backups"
    )
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        default="./config/config.json",
        help="location of config file",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        dest="db",
        default="./db",
        help="Path to download db backups too",
    )
    args = parser.parse_args()
    with open(args.config) as data:
        config = load(data)
        config["migrations"] = MIGRATIONS
        loop = get_event_loop()
        loop.run_until_complete(warehouse(config, dev=args.dev, db_path=args.db))
        loop.close()


if __name__ == "__main__":
    cli()
