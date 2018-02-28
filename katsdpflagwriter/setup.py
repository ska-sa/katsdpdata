#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="katsdpflagwriter",
    description="Karoo Array Telescope Flag Writer",
    author="Simon Ratcliffe",
    packages=find_packages(),
    scripts=[
        "scripts/flag_writer.py"
        ],
    dependency_links=[
        'git+ssh://git@github.com/ska-sa/katsdptelstate#egg=katsdptelstate',
        'git+ssh://git@github.com/ska-sa/katversion#egg=katversion'
    ],
    setup_requires=['katversion'],
    install_requires=[
        'h5py',
        'numpy',
        'dask[array]',
        'spead2>=1.5.0',     # For stop_on_stop_item
        'katcp',
        'katdal',
        'katsdptelstate',
        'katsdpservices',
        'hiredis',
        'netifaces',
        'futures'
    ],
    use_katversion=True
)
