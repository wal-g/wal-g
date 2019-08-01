"""
Steps related to backups.
"""
import os, re

import yaml
from behave import then, when
from hamcrest import (assert_that, contains_string, equal_to, has_entries, matches_regexp)

from helpers import docker, mongodb
from helpers.utils import context_to_dict, render_template


@when('we create {node_name} backup')
def step_create_backup(context, node_name):
    options = yaml.load(context.text or '') or {}
    backup_id_regexp = '[0-9]{8}T[0-9]{6}'
    cmd_args = []
#   labels are not supported by wal-g for now
#    labels = options.get('labels', {}) 
#    for key, value in labels.items():
#        cmd_args.append('--label {0}={1}'.format(key, value))

    backup_id = options.get('name')
    if backup_id:
        cmd_args.append('--name {0}'.format(backup_id))
        backup_id_regexp = '^{0}$'.format(backup_id)

    current_backup_id = mongodb.make_backup(docker.get_container(context, node_name), cmd_args=' '.join(cmd_args))
    context.safe_storage['created_backup_names'].append(current_backup_id)
    assert_that(current_backup_id, matches_regexp(backup_id_regexp))


@then('we got {backups_count} backup entries of {node_name}')
def step_check_backup_entries_count(context, backups_count, node_name):
    backup_names = mongodb.get_backup_entries(docker.get_container(context, node_name))

    current_backups_count = sum(
        [  # pylint: disable=expression-not-assigned
            (1 if re.match(r'stream_[0-9]{8}T[0-9]{6}Z', backup_id) else 0) for backup_id in backup_names
        ]
    )

    assert_that(current_backups_count, equal_to(int(backups_count)))


@then('backup list of {node_name} is')
def step_check_backup_list(context, node_name):
    templ_data = render_template(context.text, context_to_dict(context))
    conditions = yaml.load(templ_data)

    backup_names = mongodb.get_backup_entries(docker.get_container(context, node_name))
    assert_that(backup_names, equal_to(conditions))


@when('we restore #{backup_num} backup to {node_name}')
def step_restore_backup(context, backup_num, node_name):
    instance = docker.get_container(context, node_name)
    backup_id = mongodb.restore_backup_num(instance, int(backup_num))
    assert_that(backup_id, matches_regexp('^[0-9]{8}T[0-9]{6}$'))


@when('we delete #{backup_num} backup via {node_name}')
def step_delete_backup(context, backup_num, node_name):
    """
    Delete specified backup // not supported by wal-g for now
    """
    instance = docker.get_container(context, node_name)
    backup_id = mongodb.delete_backup_num(instance, int(backup_num))
    assert_that(backup_id, matches_regexp('^[0-9]{8}T[0-9]{6}$'))


@when('we delete backups retain {keep_number} via {node_name}')
def step_purge_backups(context, node_name, keep_number):
    """
    Delete specified backup
    """
    instance = docker.get_container(context, node_name)
    backup_ids = mongodb.purge_backups(instance, keep_number)
    assert_that(backup_ids, matches_regexp('([0-9]{8}T[0-9]{6}\n)*'))


@then('we ensure {node_name} #{backup_num:d} backup metadata contains')
def step_backup_metadata(context, node_name, backup_num):
    expected_meta = yaml.load(context.text)
    instance = docker.get_container(context, node_name)
    backup_meta = mongodb.get_backup_meta_num(instance, int(backup_num))
    assert_that(backup_meta, has_entries(expected_meta))


@when('we put empty backup via {node_name}')
def step_put_nometa_backup(context, node_name):
    """
    Create backup without meta
    """
    instance = docker.get_container(context, node_name)

    backup_name = '20010203T040506'
    bucket_name = context.conf['dynamic']['s3']['bucket']
    backup_root_dir = context.conf['dynamic']['wal-g']['path']

    backup_dir = '/export/{bucket_name}/{backup_dir}/{backup_name}'.format(
        bucket_name=bucket_name, backup_dir=backup_root_dir, backup_name=backup_name)
    backup_dump_path = os.path.join(backup_dir, 'mongodump.archive')

    context.safe_storage['nometa_backup_names'].append(backup_name)

    docker.exec_run(instance, 'mkdir -p {dir}'.format(dir=backup_dir))
    docker.exec_run(instance, 'touch {dump}'.format(dump=backup_dump_path))


@then('we check if empty backups were purged via {node_name}')
def step_check_nometa_purged(context, node_name):
    """
    Check if saved nometa backups were purged
    """
    instance = docker.get_container(context, node_name)

    bucket_name = context.conf['dynamic']['s3']['bucket']
    backup_root_dir = context.conf['dynamic']['wal-g']['path']

    for backup_name in context.safe_storage['nometa_backup_names'].pop():
        backup_dir = os.path.join('/export', bucket_name, backup_root_dir, backup_name)
        _, output = docker.exec_run(instance, 'ls {path}'.format(path=backup_dir), check=False)
        assert_that(output, contains_string('No such file or directory'))
