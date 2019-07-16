"""
Docker Compose helpers
"""

import copy
import os
import random
import shlex
import subprocess

import yaml

from . import utils

# Default invariant config
BASE_CONF = {
    'version': '2',
    'networks': {
        'test_net': {
            'external': {
                'name': '{network}',
            },
        },
    },
    'services': {},
}


@utils.env_stage('create', fail=True)
def build_images(conf, **_extra):
    """
    Build docker images.
    """
    assert conf, '"conf" must be non-empty dict'

    _call_compose(conf, 'build')


@utils.env_stage('start', fail=True)
def startup_containers(conf, **_extra):
    """
    Start up docker containers.
    """
    assert conf, '"conf" must be non-empty dict'

    _call_compose(conf, 'up -d')


@utils.env_stage('restart', fail=True)
def recreate_containers(conf, **_extra):
    """
    Recreate and restart docker containers.
    """
    assert conf, '"conf" must be non-empty dict'

    _call_compose(conf, 'up -d --force-recreate')


@utils.env_stage('stop', fail=False)
def shutdown_containers(conf, **_extra):
    """
    Shutdown and remove docker containers.
    """
    assert conf, '"conf" must be non-empty dict'

    _call_compose(conf, 'down')


@utils.env_stage('create', fail=True)
def create_config(conf, **_extra):
    """
    Generate config file and write it.
    """
    assert conf, '"conf" must be non-empty dict'

    staging_dir = _get_staging_dir(conf)
    compose_conf_path = _get_config_path(conf, staging_dir)
    # Create this directory now, otherwise if docker does
    # it later, it will be owned by root.
    compose_conf = _generate_config_dict(
        projects=conf.get('projects', {}),
        network_name=conf['network_name'],
        basedir=staging_dir,
    )
    return _write_config(compose_conf_path, compose_conf)


def read_config(conf):
    """
    Reads compose config into dict.
    """
    with open(_get_config_path(conf)) as conf_file:
        return yaml.load(conf_file)


def _write_config(path, compose_conf):
    """
    Dumps compose config into a file in Yaml format.
    """
    assert isinstance(compose_conf, dict), 'compose_conf must be a dict'

    catalog_name = os.path.dirname(path)
    os.makedirs(catalog_name, exist_ok=True)
    temp_file_path = '{dir}/.docker-compose-conftest-{num}.yaml'.format(
        dir=catalog_name,
        num=random.randint(0, 100),
    )
    with open(temp_file_path, 'w') as conf_file:
        yaml.dump(
            compose_conf,
            stream=conf_file,
            default_flow_style=False,
            indent=4,
        )
    try:
        _validate_config(temp_file_path)
        os.rename(temp_file_path, path)
    except subprocess.CalledProcessError as err:
        raise RuntimeError('unable to write config: validation failed with %s' % err)
    # Remove config only if validated ok.
    _remove_config(temp_file_path)


def _get_staging_dir(conf):
    return conf.get('staging_dir', 'staging')


def _get_config_path(conf, staging_dir=None):
    """
    Return file path to docker compose config file.
    """
    if not staging_dir:
        staging_dir = _get_staging_dir(conf)
    return os.path.join(staging_dir, 'docker-compose.yml')


def _remove_config(path):
    """
    Removes a config file.
    """
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def _validate_config(config_path):
    """
    Perform config validation by calling `docker-compose config`
    """
    _call_compose_on_config(config_path, '__config_test', 'config')


def _generate_config_dict(projects, network_name, basedir):
    """
    Create docker-compose.yml with initial images
    """
    assert isinstance(projects, dict), 'projects must be a dict'

    compose_conf = copy.deepcopy(BASE_CONF)
    # Set net name at global scope so containers will be able to reference it.
    compose_conf['networks']['test_net']['external']['name'] = network_name
    # Generate service config for each project`s instance
    # Also relative to config file location.
    for name, props in projects.items():
        instances = props.get('docker_instances', 1)
        # Zero means no images, right?
        if not instances:
            continue
        # Skip local-only projects
        if props.get('localinstall', False):
            continue
        # This num is also used in hostnames, later in
        # generate_service_dict()
        for num in range(1, instances + 1):
            instance_name = '{name}{num:02d}'.format(
                name=name,
                num=num,
            )
            # Account for instance-specific configs, if provided.
            # 'More specific wins' semantic is assumed.
            service_props = utils.merge(
                copy.deepcopy(props),
                # A service may contain a key with instance
                # name -- this is assumed to be a more specific
                # config.
                copy.deepcopy(props.get(instance_name, {})))
            # Generate 'service' section configs.
            service_conf = _generate_service_dict(
                name=name,
                instance_name=instance_name,
                instance_conf=service_props,
                network=network_name,
                basedir=basedir,
            )
            # Fill in local placeholders with own context.
            # Useful when we need to reference stuff like
            # hostname or domainname inside of the other config value.
            service_conf = utils.format_object(service_conf, **service_conf)
            compose_conf['services'].update({instance_name: service_conf})
    return compose_conf


def _generate_service_dict(name, instance_name, instance_conf, network, basedir):
    """
    Generates a single service config based on name and
    instance config.

    All paths are relative to the location of compose-config.yaml
    (which is ./staging/compose-config.yaml by default)
    """

    # Take care of volumes
    code_volume = './code/{name}'.format(name=name)
    local_code_volume = './images/{name}/src'.format(name=name)
    # Override '/code' path if local code is present.
    if os.path.exists(os.path.join(basedir, local_code_volume)):
        code_volume = local_code_volume
    volumes = {
        # Source code -- the original cloned repository.
        'code': {
            'local': code_volume,
            'remote': '/code',
            'mode': 'rw',
        },
        # Instance configs from images
        'config': {
            'local': './images/{name}/config'.format(name=name),
            'remote': '/config',
            'mode': 'rw',
        },
    }
    volume_list = _prepare_volumes(
        volumes,
        local_basedir=basedir,
    )
    # Take care of port forwarding
    ports_list = []
    for port in instance_conf.get('expose', {}).values():
        ports_list.append(port)

    # Set build path which contains Dockerfile
    build_path = instance_conf.get('build')
    if isinstance(build_path, dict):
        build_path = build_path.copy()
    # Default is to look for Dockerfile inside the projects` dir.
    elif build_path is None:
        build_path = './code/{name}/{subdir}'.format(
            name=name,
            subdir=instance_conf.get('build_subdir', '.'),
        )
    service = {
        # The path is relative to the location of the compose config file.
        # https://docs.docker.com/compose/compose-file/#build
        # Dockerfile dir
        'build': build_path,
        'image': '{nm}:{nt}'.format(nm=name, nt=network),
        'hostname': instance_name,
        'domainname': network,
        # Networks. We use external anyway.
        'networks': instance_conf.get('networks', ['test_net']),
        # Runtime envs
        'environment': instance_conf.get('environment', []),
        # Nice container name with domain name part.
        # This results, however, in a strange rdns name:
        # the domain part will end up there twice.
        # Does not affect A or AAAA, though.
        'container_name': '%s.%s' % (instance_name, network),
        # Ports exposure
        'ports': ports_list + instance_conf.get('ports', []),
        # Config and code volumes
        'volumes': volume_list + instance_conf.get('volumes', []),
        # https://github.com/moby/moby/issues/12080
        'tmpfs': '/var/run',
        # external resolver: dns64-cache.yandex.net
        'dns': ['2a02:6b8:0:3400::1023'],
        'external_links': instance_conf.get('external_links', []),
    }

    # print('%s' % instance_conf)
    # print('%s' % instance_conf['external_links'])
    # raise Exception('')
    return service


def _prepare_volumes(volumes, local_basedir):
    """
    Form a docker-compose volume list,
    and create endpoints.
    """
    assert isinstance(volumes, dict), 'volumes must be a dict'

    volume_list = []
    for props in volumes.values():
        # "local" params are expected to be relative to
        # docker-compose.yaml, so prepend its location.
        os.makedirs(
            '{base}/{dir}'.format(
                base=local_basedir,
                dir=props['local'],
            ), exist_ok=True)
        volume_list.append('{local}:{remote}:{mode}'.format(**props))
    return volume_list


def _call_compose(conf, action):
    conf_path = '{base}/docker-compose.yml'.format(base=conf.get('staging_dir', 'staging'))
    project_name = conf['network_name']

    _call_compose_on_config(conf_path, project_name, action)


def _call_compose_on_config(conf_path, project_name, action):
    """
    Execute docker-compose action by invoking `docker-compose`.
    """
    assert isinstance(action, str), 'action arg must be a string'

    compose_cmd = 'docker-compose --file {conf} -p {name} {action}'.format(
        conf=conf_path,
        name=project_name,
        action=action,
    )

    # Note: build paths are resolved relative to config file location.
    subprocess.check_call(shlex.split(compose_cmd))
