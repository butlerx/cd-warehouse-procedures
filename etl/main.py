#!/usr/bin/env python

import sys

from restore import restore_db
from clean_up import clean_databases, reset_databases
from databases import db_host, db_password, db_user, dojos, events, users
from migrate import migrate_db
from s3 import download


def main():
    reset_databases()
    download('dojos')
    restore_db(db_host, dojos, db_user, db_password, '/db/dojos.tar.gz')
    download('events')
    restore_db(db_host, events, db_user, db_password, '/db/events.tar.gz')
    download('users')
    restore_db(db_host, users, db_user, db_password, '/db/users.tar.gz')
    migrate_db()
    clean_databases()
    print("data migrated")
    sys.exit(0)


if __name__ == '__main__':
    main()
