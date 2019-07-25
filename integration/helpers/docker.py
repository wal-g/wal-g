"""
Docker-helpers for tests.
This module defines functions that facilitate the interaction with docker,
e.g. creating or shutting down an external network.
"""

import os
import random
import subprocess
from distutils import dir_util

import docker

from . import utils

DOCKER_API = docker.from_env()


def get_container(context, prefix):
    """
    Get container object by prefix
    """
    return DOCKER_API.containers.get('%s.%s' % (prefix, context.conf['network_name']))


def get_exposed_port(container, port):
    """
    Get pair of (host, port) for connection to exposed port
    """
    machine_name = os.getenv('DOCKER_MACHINE_NAME')
    if machine_name:
        host = subprocess.check_output(['docker-machine', 'ip', machine_name]).decode('utf-8').rstrip()
    else:
        host = 'localhost'

    binding = container.attrs['NetworkSettings']['Ports'].get('%d/tcp' % port)

    if binding:
        return host, binding[0]['HostPort']


def exec_run(instance, cmd, tty=None, user=None, check=False):
    """
    Run command inside container and return exit_code and decoded output
    """
    exit_code, raw_output = instance.exec_run(cmd, tty=tty, user=user)
    output = str(raw_output.decode())
    if exit_code and check is True:
        raise RuntimeError('Failed exec run of "{0}": {1}'.format(cmd, output))
    return exit_code, output


def generate_ipv6(subnet=None):
    """
    Generates a random IPv6 address in the provided subnet.
    """
    if subnet is None:
        subnet = 'fd00:dead:beef:%s::/96'
    random_part = ':'.join(['%x' % random.randint(0, 16**4) for _ in range(3)])
    return subnet % random_part


def generate_ipv4(subnet=None):
    """
    Generates a random IPv4 address in the provided subnet.
    """
    if subnet is None:
        subnet = '10.%s.0/24'
    random_part = '.'.join(['%d' % random.randint(0, 255) for _ in range(2)])
    return subnet % random_part


@utils.env_stage('create', fail=True)
def prep_images(state, conf):
    """
    Prepare base images.
    """
    for arg in (state, conf):
        assert arg is not None, '%s must not be None' % arg

    # Copy docker-files and configs to staging dir
    images_dir = conf.get('images_dir', 'images')
    staging_dir = conf.get('staging_dir', 'staging')
    dir_util.copy_tree(
        images_dir,
        '{staging}/{images}'.format(staging=staging_dir, images=images_dir),
        update=True,
    )

    for props in conf.get('base_images', {}).values():
        DOCKER_API.images.build(**props)


@utils.env_stage('create', fail=True)
def prep_network(state, conf):
    """
    Creates ipv6-enabled docker network with random name and address space
    """
    for arg in (state, conf):
        assert arg is not None, '%s must not be None' % arg

    # Unfortunately docker is retarded and not able to create
    # ipv6-only network (see https://github.com/docker/libnetwork/issues/1192)
    # Do not create new network if there is an another net with the same name.
    if DOCKER_API.networks.list(names='^%s$' % conf.get('network_name')):
        return
    ip_subnet_pool = docker.types.IPAMConfig(pool_configs=[
        docker.types.IPAMPool(subnet=generate_ipv4(conf.get('docker_ip4_subnet'))),
        docker.types.IPAMPool(subnet=generate_ipv6(conf.get('docker_ip6_subnet'))),
    ])
    bridge_name = '{name}_{num}'.format(
        name=conf.get('docker_bridge_name', 'dbaas'),
        num=random.randint(0, 65535),
    )
    net_name = conf.get('network_name', 'test_net_%s' % bridge_name)
    net_opts = {
        'com.docker.network.bridge.enable_ip_masquerade': 'true',
        'com.docker.network.bridge.enable_icc': 'true',
        'com.docker.network.bridge.name': bridge_name,
    }
    DOCKER_API.networks.create(
        net_name,
        options=net_opts,
        enable_ipv6=True,
        ipam=ip_subnet_pool,
    )


@utils.env_stage('stop', fail=False)
def shutdown_network(conf, **_extra):
    """
    Stop docker network(s)
    """
    nets = DOCKER_API.networks.list(names=conf.get('network_name', '^test_net_'))
    for net in nets:
        net.remove()
