import os
import shlex
from subprocess import STDOUT, Popen


def restore_db(host, database, user, password, name):
    path = '/db/' + name + '.tar.gz'
    directory = '/db/' + name
    if not os.path.exists(directory):
        os.makedirs(directory)
    tar = Popen(
        shlex.split('tar xvf {0} -C {1}'.format(path, directory)),
        shell=False,
        stderr=STDOUT)
    pg_env = os.environ.copy()
    pg_env["PATH"] = "/usr/sbin:/sbin:" + pg_env["PATH"]
    pg_env["PGPASSWORD"] = password
    tar.wait()
    pg = Popen(
        shlex.split(
            'pg_restore -c --if-exists -w -h {0} -d {1} -U {2} {3}/backup_dump'.
            format(host, database, user, directory)),
        shell=False,
        stderr=STDOUT,
        env=pg_env)
    pg.wait()
