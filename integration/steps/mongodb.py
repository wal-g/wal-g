"""
Steps related to mongodb.
"""

from behave import given, then
from hamcrest import assert_that, equal_to
from retrying import retry

from helpers import docker, mongodb, utils


@given('a working mongodb on {node_name}')
@retry(wait_fixed=200, stop_max_attempt_number=25)
def step_wait_for_mongodb_alive(context, node_name):
    """
    Wait until mongodb is ready to accept incoming requests.
    """
    conn = mongodb.mongod_connect(context, node_name, creds=False)
    ping = conn['test'].command('ping')
    if ping['ok'] != 1:
        raise RuntimeError('MongoDB is not alive')


@given('{node_name} has test mongodb data {test_name}')
def step_fill_with_test_data(context, node_name, test_name):
    """
    Load test data to mongodb
    """
    creds = context.conf['projects']['mongodb']['users']['admin']
    conn = mongodb.mongod_connect(context, node_name, creds)
    data = mongodb.fill_with_data(conn, mark=test_name)
    if not hasattr(context, 'test_data'):
        context.test_data = {}

    context.test_data[test_name] = data


@given('mongodb replset initialized on {node_name}')
@retry(wait_fixed=1000, stop_max_attempt_number=15)
def step_ensure_rs_initialized(context, node_name):
    """
    Initialize replicaset
    """
    instance = docker.get_container(context, node_name)
    cmd = utils.strip_query("""
        mongo --host localhost --quiet --norc --port 27018
            --eval "rs.status()"
    """)

    _, rs_status_output = docker.exec_run(instance, cmd)
    if 'myState' in rs_status_output:
        return

    if 'NotYetInitialized' in rs_status_output:
        cmd = utils.strip_query("""
                mongo --host localhost --quiet --norc --port 27018
                    --eval "rs.initiate()"
            """)
        docker.exec_run(instance, cmd)
    elif 'Unauthorized' in rs_status_output:
        creds = context.conf['projects']['mongodb']['users']['admin']
        conn = mongodb.mongod_connect(context, node_name, creds)
        if mongodb.check_rs_initialized(conn):
            return
    raise RuntimeError('Replset was not initialized: {0}'.format(rs_status_output))


@given('mongodb role is primary on {node_name}')
@retry(wait_fixed=1000, stop_max_attempt_number=15)
def step_mongodb_is_primary(context, node_name):
    creds = context.conf['projects']['mongodb']['users']['admin']
    conn = mongodb.mongod_connect(context, node_name, creds)
    if not conn.is_primary:
        raise RuntimeError('Node is not a primary')


@given('mongodb auth initialized on {node_name}')
def step_ensure_auth_initialized(context, node_name):
    """
    Create mongodb admin user
    """
    creds = context.conf['projects']['mongodb']['users']['admin']
    instance = docker.get_container(context, node_name)
    cmd = utils.strip_query("""
        mongo --host localhost --quiet --norc --port 27018
            --eval "db.createUser({{
                user: '{username}',
                pwd: '{password}',
                roles: {roles}}})"
        {dbname}
    """.format(**creds))
    _, output = docker.exec_run(instance, cmd)
    if all(log not in output for log in ('not authorized on admin', 'Successfully added user')):
        raise RuntimeError('Can not initialize auth: {0}'.format(output))


@then('we got same mongodb data at {nodes_list}')
def step_same_mongodb_data(context, nodes_list):
    creds = context.conf['projects']['mongodb']['users']['admin']
    user_data = []
    for node_name in nodes_list.split():
        conn = mongodb.mongod_connect(context, node_name, creds)
        rows_data = mongodb.get_all_user_data(conn)
        user_data.append(rows_data)

    node1_data = user_data[0]
    assert_that(node1_data)

    for node_num in range(1, len(user_data)):
        node_data = user_data[node_num]
        assert_that(node_data, equal_to(node1_data))
