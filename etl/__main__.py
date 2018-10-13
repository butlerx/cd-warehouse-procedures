#! /usr/bin/env python3

from argparse import ArgumentParser
from asyncio import get_event_loop

from .migrate import migrate


def cli():
    """cli interface"""
    parser = ArgumentParser(
        prog="etl",
        description="migrate production databases backups to datawarehouse")
    parser.add_argument(
        "--dev", action="store_true", help="dev mode to use local backups")
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
    return parser.parse_args()


if __name__ == "__main__":
    args = cli()
    try:
        # For production, use libuv as our eventloop as it is *FAR*
        # more performant than the default asyncio event loop.
        # https://magic.io/blog/uvloop-blazing-fast-python-networking/
        from uvloop import EventLoopPolicy
        from asyncio import set_event_loop_policy

        set_event_loop_policy(EventLoopPolicy())
        print("Using uvloop for asyncio")
    except Exception:
        pass
    loop = get_event_loop()
    loop.run_until_complete(
        migrate(args.config, dev=args.dev, db_path=args.db))
    loop.close()
