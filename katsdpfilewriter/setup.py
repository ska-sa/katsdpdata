#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="katsdpfilewriter",
    version="trunk",
    description="Karoo Array Telescope Data Capture",
    author="Bruce Merry",
    packages=find_packages(),
    scripts=[
        "scripts/file_writer.py"
        ],
    dependency_links=[
        'git+ssh://git@github.com/ska-sa/katsdptelstate#egg=katsdptelstate',
        'git+https://github.com/ska-sa/PySPEAD#egg=spead'
    ],
    install_requires=[
        'h5py',
        'numpy',
        'spead2',
        'katcp',
        'katsdptelstate'
    ]
)
