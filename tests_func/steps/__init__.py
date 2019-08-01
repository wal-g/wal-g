"""
Module with definition of test steps.
"""

import parse
from behave import register_type


@parse.with_pattern('.*?')
def parse_optional(text):
    """
    Parse function for optional parameters. Unlike default parse type (with
    regexp '.+?'), it also accepts empty strings.
    """
    return text.strip()


register_type(optional=parse_optional)
