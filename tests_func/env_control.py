#!/usr/bin/env python3
"""
Manage test environment.

For config, see configuration.py.
"""

import argparse
import logging
import pickle

import configuration
# This modules define stages to prepare the environment
from helpers import compose, docker, templates

SESSION_STATE_CONF = '.session_conf.sav'
STAGES = {
    'create': [
        # The order here is important: stages depend on previous` results.
        # e.g. you wont get much success building from docker-compose
        # unless you have base image in place.

        # Prepare base image(s)
        docker.prep_images,
        # Create docker containers` network to enable
        # cross-container network communication.
        docker.prep_network,
        # Generate docker-compose.yml
        compose.create_config,
        # Render configs using all available contexts
        templates.render_configs,
        # Build docker images
        compose.build_images,
    ],
    'start': [compose.startup_containers],
    'restart': [compose.recreate_containers],
    'stop': [
        # Shutdown docker containers
        compose.shutdown_containers,
        # Remove network bridges
        docker.shutdown_network,
    ],
}


def _run_stage(event, state=None, state_file=None):
    """
    Run stage steps.
    """
    assert event in STAGES, event + ' not implemented'

    if not state:
        state = _init_state(state_file)

    for step in STAGES[event]:
        step(state=state, conf=state['config'])

    return state


def create(state_file=None):
    """
    Create test environment.
    """
    state = _run_stage('create', state_file=state_file)

    _save_state({
        'config': state['config'],
    }, path=state_file)

    return state


def start(state=None, state_file=None):
    """
    Start test environment runtime.
    """
    _run_stage('start', state=state, state_file=state_file)


def restart(state=None, state_file=None):
    """
    Restart test environment runtime.
    """
    _run_stage('restart', state=state, state_file=state_file)


def stop(state=None, state_file=None):
    """
    Stop test environment runtime.
    """
    _run_stage('stop', state=state, state_file=state_file)


def _init_state(state_file=None):
    """
    Create state.
    If previous state file is found, restore it.
    If not, create new.
    """
    if state_file is None:
        state_file = SESSION_STATE_CONF
    state = {
        'config': None,
    }
    # Load previous state if found.
    try:
        with open(state_file, 'rb') as session_conf:
            return pickle.load(session_conf)
    except FileNotFoundError:
        # Clean slate: only need config for now, as
        # other stuff will be defined later.
        state['config'] = configuration.get()
    return state


def _save_state(conf, path=None):
    """
    Pickle state to disk.
    """
    if path is None:
        path = SESSION_STATE_CONF
    with open(path, 'wb') as session_conf:
        pickle.dump(conf, session_conf)


def parse_args(commands):
    """
    Parse command-line arguments.
    """
    arg = argparse.ArgumentParser(description="""
        testing environment initializer script
        """)
    arg.add_argument(
        'command',
        choices=list(commands),
        help='command to perform',
    )
    arg.add_argument(
        '-s',
        '--state-file',
        dest='state_file',
        type=str,
        metavar='<path>',
        default=SESSION_STATE_CONF,
        help='path to state file (pickle dump)',
    )
    return arg.parse_args()


def cli_main():
    """
    CLI entry.
    """
    commands = {
        'create': create,
        'start': start,
        'stop': stop,
    }

    logging.basicConfig(
        format='%(asctime)s [%(levelname)s]:\t%(message)s',
        level=logging.INFO,
    )
    args = parse_args(commands)
    commands[args.command](state_file=args.state_file)


if __name__ == '__main__':
    cli_main()
