#!/usr/bin/env python3
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
        'numpy',
        'spead2>=1.7.1',     # asyncio needed
        'aiokatcp>=0.3',     # status_func needed
        'katsdptelstate',
        'katsdpservices'
    ],
    use_katversion=True
)
