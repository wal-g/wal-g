"""
Utilities for dealing with genbackup
"""

import json
from datetime import datetime
from urllib.parse import quote_plus

import pymongo

from . import crypto, docker

DB_COUNT = 1
TABLE_COUNT = 1
ROWS_COUNT = 1

WALG_CLI_PATH = '/usr/bin/wal-g'
WALG_CONF_PATH = '/home/.walg.json'
WALG_DEFAULT_ARGS = ''


def mongod_connect(context, node_name, creds=None):
    """
    Connect to mongodb hostname given a config.
    Returns pymongo.MongoClient instance
    """

    host, port = docker.get_exposed_port(
        docker.get_container(context, node_name), context.conf['projects']['mongodb']['expose']['mongod'])

    if creds:
        connect_uri = 'mongodb://{user}:{password}@{host}:{port}/{dbname}'.\
            format(
                user=quote_plus(creds['username']),
                password=quote_plus(creds['password']),
                host=host,
                port=port,
                dbname=quote_plus(creds['dbname']))
    else:
        connect_uri = 'mongodb://{host}:{port}'.format(host=host, port=port)

    return pymongo.MongoClient(connect_uri)


def ensure_auth_initialized(conn, user):
    """
    Initialize auth by creating admin user
    """
    try:
        mdb = pymongo.database.Database(conn, user['dbname'])
        mdb.add_user(user['username'], user['password'], roles=user['roles'])
    except pymongo.errors.PyMongoError as exc:
        if 'not authorized on' in str(exc):
            return
        raise RuntimeError('Unable to add user: {0}'.format(exc))


def ensure_user_exists(conn, user):
    """
    Create mongodb user
    """
    try:
        mdb = pymongo.database.Database(conn, user['dbname'])
        return mdb.command("usersInfo", user['username'])['users']

    except pymongo.errors.OperationFailure:
        try:
            mdb = pymongo.database.Database(conn, user['dbname'])
            return mdb.add_user(user['username'], user['password'], roles=user['roles'])
        except pymongo.errors.PyMongoError as err:
            raise RuntimeError('Unable to add user: {0}'.format(err))

    except pymongo.errors.PyMongoError as err:
        raise RuntimeError('Unable to add user: {0}'.format(err))


def fill_with_data(conn, mark=None):
    """
    Fill test schema with data
    """
    data = {}
    if mark is None:
        mark = ''
    for db_num in range(1, DB_COUNT + 1):
        db_name = 'test_db_{db_num:02d}'.format(db_num=db_num)
        data[db_name] = {}
        for table_num in range(1, TABLE_COUNT + 1):
            rows = []
            table_name = 'test_table_{table_num:02d}'.\
                format(table_num=table_num)
            for row_num in range(1, ROWS_COUNT + 1):
                rows.append(gen_record(row_num=row_num, str_prefix=mark))

            conn[db_name][table_name].insert_many(rows)
            data[db_name][table_name] = rows[:]
    return data


def gen_record(row_num=0, str_len=5, str_prefix=None):
    """
    Generate test record
    """

    if str_prefix is None:
        str_prefix = ''
    else:
        str_prefix = '{prefix}_'.format(prefix=str_prefix)

    rand_str = crypto.gen_plain_random_string(str_len)
    return {
        'datetime': datetime.now(),
        'int_num': row_num,
        'str': '{prefix}{rand_str}'.format(prefix=str_prefix, rand_str=rand_str),
    }


def make_backup(instance, cli_path=None, conf_path=None, cmd_args=None):
    """
    Call backup cli to run backup
    """

    if cli_path is None:
        cli_path = WALG_CLI_PATH
    if conf_path is None:
        conf_path = WALG_CONF_PATH
    if cmd_args is None:
        cmd_args = WALG_DEFAULT_ARGS
    backup_command =  '{cli_path} --config {conf_path} stream-push {args}'.format(
            cli_path=cli_path, args=cmd_args, conf_path=conf_path)
    print(backup_command)
    _, output = docker.exec_run(
        instance, backup_command)
    return output


def restore_backup_num(instance, backup_num):
    """
    Call backup cli to run restore backup by serial number
    """
    backup_entries = get_backup_entries(instance)
    return restore_backup_entry(instance, backup_entries[backup_num])


def delete_backup_num(instance, backup_num):
    """
    Call backup cli to run delete backup by serial number
    """
    backup_entries = get_backup_entries(instance)
    return delete_backup_entry(instance, backup_entries[backup_num])


def get_backup_meta_num(instance, backup_num):
    """
    Call backup cli to get backup meta by serial number
    """
    backup_entries = get_backup_entries(instance)
    return get_backup_meta_entry(instance, backup_entries[backup_num])


#    def get_backup_meta_entry(instance, backup_entry, cli_path=None, conf_path=None):
#        """
#        Call backup cli to run delete backup entry
#        """
#        if cli_path is None:
#            cli_path = WALG_CLI_PATH
#        if conf_path is None:
#            conf_path = WALG_CONF_PATH
#        _, output = docker.exec_run(
#            instance, '{cli_path} -c {conf_path} -p {backup_entry} show'.format(
#                cli_path=cli_path, conf_path=conf_path, backup_entry=backup_entry))
#        return json.loads(output)


def delete_backup_entry(instance, backup_entry, cli_path=None, conf_path=None):
    """
    Call backup cli to run delete backup entry
    """
    if cli_path is None:
        cli_path = WALG_CLI_PATH
    if conf_path is None:
        conf_path = WALG_CONF_PATH
    _, output = docker.exec_run(
        instance, '{cli_path} --config {conf_path} delete retain 1 --confirm'.format(
            cli_path=cli_pathi, conf_path=conf_path))
    return output


def purge_backups(instance, number, cli_path=None, conf_path=None):
    """
    Call backup cli to run purge backups
    """
    if cli_path is None:
        cli_path = WALG_CLI_PATH
    if conf_path is None:
        conf_path = WALG_CONF_PATH
    _, output = docker.exec_run(instance, '{cli_path} --config {conf_path} delete retain {number} --confirm'.format(
        cli_path=cli_path, number=number, conf_path=conf_path))
    print(output)
    return output


def get_backup_entries(instance, cli_path=None, conf_path=None):
    """
    Call backup cli to retrieve existing backup entries
    """

    if cli_path is None:
        cli_path = WALG_CLI_PATH
    if conf_path is None:
        conf_path = WALG_CONF_PATH
    command = '{cli_path} --config {conf_path} backup-list'.format(
        cli_path=cli_path, conf_path=conf_path)
    _, output = docker.exec_run(instance, command)
    raw_entries = output.split('\n')
    return list(filter(None, raw_entries))


def restore_backup_entry(instance, backup_entry, cli_path=None, conf_path=None):
    """
    Call backup cli to run restore backup entry
    """

    if cli_path is None:
        cli_path = WALG_CLI_PATH
    if conf_path is None:
        conf_path = WALG_CONF_PATH
    print(backup_entry)    
    _, output = docker.exec_run(
        instance, '{cli_path} --config {conf_path} stream-fetch {backup_entry}'.format(
            cli_path=cli_path, backup_entry=backup_entry, conf_path=conf_path))
    return output


def get_all_user_data(conn):
    """
    Retrieve all user data
    """

    user_data = []
    for db in sorted(conn.database_names()):
        tables = conn[db].collection_names(include_system_collections=False)
        for table in sorted(tables):
            if db == 'local':
                continue
            for row in conn[db][table].find():
                user_data.append((db, table, row))
    return user_data


def check_rs_initialized(conn):
    """
    Check replicaset is initialized
    """

    try:
        return bool(conn.admin.command('replSetGetStatus'))
    except pymongo.errors.PyMongoError as err:
        if 'no replset config has been received' in str(err):
            return False
        raise RuntimeError('Unable retrieve replset status: {0}'.format(err))
