"""
Behave entry point.

For details of env bootstrap, see env_control
"""
import logging

import env_control

SAFE_STORAGE = {
    'created_backup_names': [],
    'nometa_backup_names': [],
}


def before_all(context):
    """
    Prepare environment for tests.
    """
    context.state = env_control.create()
    context.conf = context.state['config']


def before_feature(context, _feature):
    """
    Cleanup function executing per feature.
    """
    env_control.restart(state=context.state)


def before_step(context, _):
    """
    Launch debug before step
    """
    context.safe_storage = SAFE_STORAGE


def after_all(context):
    """
    Clean up.
    """
    if context.failed and not context.aborted:
        logging.warning('Remember to run `make clean` after you done')
        return
    env_control.stop(state=context.state)
