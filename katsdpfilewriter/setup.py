#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="katsdpfilewriter",
    description="Karoo Array Telescope Data Capture",
    author="Bruce Merry",
    packages=find_packages(),
    scripts=[
        "scripts/file_writer.py"
        ],
    dependency_links=[
        'git+ssh://git@github.com/ska-sa/katsdptelstate#egg=katsdptelstate',
        'git+ssh://git@github.com/ska-sa/katversion#egg=katversion'
    ],
    setup_requires=['katversion'],
    install_requires=[
        'h5py',
        'numpy',
        'spead2>=0.5.0',
        'katcp',
        'katsdptelstate',
        'hiredis',
        'netifaces'
    ],
    use_katversion=True
)
