"""restoring from backups"""
from os import environ, makedirs, path
from shlex import split
from subprocess import DEVNULL, STDOUT, Popen

from clean_up import Connection


def restore_db(con: Connection, database: str, name: str) -> None:
    """restore a pg backup stored in /db/"""
    directory = '/db/{}'.format(name)
    if not path.exists(directory):
        makedirs(directory)
    tar = Popen(
        split('tar xvf {0} -C {1}'.format('{}.tar.gz'.format(directory),
                                          directory)),
        shell=False,
        stdout=DEVNULL,
        stderr=STDOUT)
    pg_env = environ.copy()
    pg_env["PATH"] = "/usr/sbin:/sbin:{}".format(pg_env["PATH"])
    pg_env["PGPASSWORD"] = con.password
    tar.wait()
    cmd = 'pg_restore -c --if-exists -w -h {0} -d {1} -U {2} {3}/backup_dump'
    postgres = Popen(
        split(
            cmd.format(con.host, database, con.user, directory)),
        shell=False,
        stderr=STDOUT,
        env=pg_env)
    postgres.wait()
