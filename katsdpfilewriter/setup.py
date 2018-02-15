#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="katsdpfilewriter",
    description="Karoo Array Telescope Data Capture",
    author="Bruce Merry",
    packages=find_packages(),
    scripts=[
        "scripts/file_writer.py",
        "scripts/vis_writer.py"
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
