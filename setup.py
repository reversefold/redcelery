#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='redcelery',
    version='3.1.18',
    description=('Patches for celery which attempt to mitigate issues with its use of redis connections '
                 'causing workers to hang'),
    author='Justin Patrin',
    author_email='papercrane@reversefold.com',
    url='https://github.com/reversefold/redcelery',
    packages=find_packages(),
    license='MIT',
    install_requires=[
        'celery==3.1.18',
        'redis',
    ],
)
