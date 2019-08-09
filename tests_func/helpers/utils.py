"""
General purpose stuff, like dict merging or str.format() template filler.
"""
import collections
import logging
import re
from functools import wraps

import jinja2


def merge(original, update):
    """
    Recursively merge update dict into original.
    """
    for key in update:
        recurse_conditions = [
            # Does update have same key?
            key in original,
            # Do both the update and original have dicts at this key?
            isinstance(original.get(key), dict),
            isinstance(update.get(key), collections.Mapping),
        ]
        if all(recurse_conditions):
            merge(original[key], update[key])
        else:
            original[key] = update[key]
    return original


def format_object(obj, **replacements):
    """
    Replace format placeholders with actual values
    """
    if isinstance(obj, str):
        obj = obj.format(**replacements)
    elif isinstance(obj, collections.Mapping):
        for key, value in obj.items():
            obj[key] = format_object(value, **replacements)
    elif isinstance(obj, collections.Iterable):
        for idx, val in enumerate(obj):
            obj[idx] = format_object(val, **replacements)
    return obj


def env_stage(event, fail=False):
    """
    Nicely logs env stage
    """

    def wrapper(fun):
        @wraps(fun)
        def wrapped_fun(*args, **kwargs):  # pylint: disable=missing-docstring
            stage_name = '{mod}.{fun}'.format(
                mod=fun.__module__,
                fun=fun.__name__,
            )
            logging.info('initiating %s stage %s', event, stage_name)
            try:
                return fun(*args, **kwargs)
            except Exception as exc:
                logging.error('%s failed: %s', stage_name, exc)
                if fail:
                    raise

        return wrapped_fun

    return wrapper


def strip_query(query_text):
    """
    Remove query without endlines and duplicate whitespaces
    """
    return re.sub(r'\s{2,}', ' ', query_text.replace('\n', ' ')).strip()


def render_template(template, template_context):
    """
    Render jinja template
    """
    env = jinja2.Environment()
    env.filters['len'] = len
    return env.from_string(template).render(**template_context)


def context_to_dict(context):
    """
    Convert behave context to dict representation.
    """
    result = {}
    for frame in context._stack:  # pylint: disable=protected-access
        for key, value in frame.items():
            if key not in result:
                result[key] = value
    return result
