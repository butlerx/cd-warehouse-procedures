import os
import shlex
from shutil import rmtree
from subprocess import STDOUT, Popen


def restore_db(host, database, user, password, path):
    tar = Popen(
        shlex.split('tar xvf {0} -C /db'.format(path)),
        shell=False,
        stderr=STDOUT)
    my_env = os.environ.copy()
    my_env["PATH"] = "/usr/sbin:/sbin:" + my_env["PATH"]
    tar.wait()
    my_env["PGPASSWORD"] = password
    pg = Popen(
        shlex.split(
            'pg_restore -c --if-exists -w -h {0} -d {1} -U {2} /db/backup_dump'.
            format(host, database, user)),
        shell=False,
        stderr=STDOUT,
        env=my_env)
    pg.wait()
    rmtree('/db/backup_dump')
