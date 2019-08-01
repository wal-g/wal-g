"""
Steps related to s3.
"""

from behave import given
from retrying import retry

from helpers import docker


@given('a configured s3 on {node_name}')
@retry(wait_fixed=1000, stop_max_attempt_number=120)
def step_configure_s3(context, node_name):
    """
    Configure minio server bucket
    """
    instance = docker.get_container(context, node_name)

    exit_code, output = docker.exec_run(instance, 'mc admin info local', check=False)
    if exit_code and 'Access Denied.' not in output:
        raise RuntimeError('s3 is not available: {0}'.format(output))

    bucket_name = context.conf['dynamic']['s3']['bucket']
    access_key_id = context.conf['dynamic']['s3']['access_key_id']
    access_secret_key = context.conf['dynamic']['s3']['access_secret_key']
    cmd = 'mc --debug config host add local http://localhost:9000 {0} {1}'.format(access_key_id, access_secret_key)
    docker.exec_run(instance, cmd)

    exit_code, output = docker.exec_run(instance, 'mc mb local/{0}'.format(bucket_name), check=False)
    if all(log not in output for log in ('created successfully', 'already own it')):
        raise RuntimeError('Can not create bucket {0}: {1}'.format(bucket_name, output))
