"""
Steps related to gpg.
"""

from behave import given

from helpers import docker


@given('a trusted gpg keys on {node_name}')
def step_wait_for_mongodb_alive(context, node_name):
    gpg_conf = context.conf['dynamic']['gpg']
    instance = docker.get_container(context, node_name)

    _, trust_check = docker.exec_run(
        instance, 'gpg --list-keys --list-options show-uid-validity', user=gpg_conf['user'])
    if '[ultimate] test_cluster' in trust_check:
        return

    _, import_output = docker.exec_run(
        instance,
        'gpg --homedir {homedir} --no-tty --import /config/gpg-key.armor'.format(**gpg_conf),
        tty=True,
        user=gpg_conf['user'],
    )

    if 'secret keys imported: 1' not in import_output:
        raise RuntimeError('Can not import keys: {0}'.format(import_output))

    _, trust_output = docker.exec_run(
        instance,
        [
            'bash',
            '-c',
            """
            for key in $(gpg --no-tty --homedir {homedir} -k | grep ^pub |
            cut -d'/' -f2 | awk '{{print $1}};' 2>/dev/null); do
                printf "trust\n5\ny\nquit" | \
                gpg --homedir {homedir} --debug --no-tty --command-fd 0 \
                    --edit-key ${{key}};
            done
        """.format(**gpg_conf),
        ],
        tty=True,
        user=gpg_conf['user'],
    )
    _, check_result = docker.exec_run(
        instance, 'gpg --list-keys --list-options show-uid-validity', user=gpg_conf['user'])
    if '[ultimate] test_cluster' not in check_result:
        raise RuntimeError('Can not trust keys: {0}'.format(trust_output))
