"""
Common steps.
"""

import time

from behave import then, when


@then('wait for {sec_num:d} seconds')
@when('wait for {sec_num:d} seconds')
def step_create_backup(_, sec_num):
    time.sleep(sec_num)
