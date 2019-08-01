"""
Renders configs using all available context: config, docker-compose.yaml, state
"""
import os

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from . import compose, utils

TEMP_FILE_EXT = 'temp~'


@utils.env_stage('create', fail=True)
def render_configs(conf, **_extra):
    """
    Render each template in the subtree.
    Each template is rendered in-place. As the framework
    operates in staging dir, this is easily reset
    by `make clean`, or `rm -fr staging`.
    """
    compose_conf = compose.read_config(conf)
    config_root = '{staging}/images'.format(staging=conf.get('staging_dir', 'staging'))
    context = {
        'conf': conf,
        'compose': compose_conf,
    }
    # Render configs only for projects that are
    # present in config file.
    for project in conf.get('projects'):
        for root, _, files in os.walk('%s/%s' % (config_root, project)):
            for basename in files:
                if basename.endswith(TEMP_FILE_EXT):
                    continue
                render_templates_dir(
                    context,
                    root,
                    basename,
                )


def getenv(loader=None):
    """
    Create Jinja2 env object
    """
    env = Environment(
        autoescape=False,
        trim_blocks=False,
        undefined=StrictUndefined,
        keep_trailing_newline=True,
        loader=loader,
    )
    return env


def render_templates_dir(context, directory, basename):
    """
    Renders the actual template.
    """
    path = '%s/%s' % (directory, basename)
    temp_file_path = '%s.%s' % (path, TEMP_FILE_EXT)
    loader = FileSystemLoader(directory)
    env = getenv(loader)

    # Various filters, e.g. "password_clear | sha256" yields a hashed password.
    try:
        with open(temp_file_path, 'w') as temp_file:
            temp_file.write(env.get_template(basename).render(context))
    except Exception as exc:
        raise RuntimeError("'{exc_type}' while rendering '{name}': {exc}".format(
            exc_type=exc.__class__.__name__,
            name=path,
            exc=exc,
        ))
    os.rename(temp_file_path, path)
