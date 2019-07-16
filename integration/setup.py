#!/usr/bin/env python3
# encoding: utf-8
"""
setup.py for DBaaS genbackup
"""

from setuptools import setup, find_packages


REQUIREMENTS = [
    'PyYAML>=3.10',
    'pymongo>=3.6.1',
    's3cmd>=2.0.1',
    'humanfriendly==4.4.1',
]

setup(
    name='genbackup',
    version='0.0.1',
    description='DBaaS genbackup',
    license='Yandex License',
    url='https://github.yandex-team.ru/mdb/genbackup/',
    author='DBaaS team',
    author_email='mdb-admin@yandex-team.ru',
    maintainer='DBaaS team',
    maintainer_email='mdb-admin@yandex-team.ru',
    zip_safe=False,
    platforms=['Linux', 'BSD', 'MacOS'],
    packages=find_packages(exclude=['tests*']),
    install_requires=REQUIREMENTS,
)
