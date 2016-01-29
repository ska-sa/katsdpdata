#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name = "katsdpdataclient",
    description = "Data search and retrieval client library for the MeerKAT project",
    author = "Thomas Bennett, Chris Schollar",
    author_email = "thomas@ska.ac.za, cschollar@ska.ac.za",
    packages = find_packages(),
    url = 'http://ska.ac.za/',
    scripts = [
        "scripts/celery-workflowmgr.py",
        "scripts/workflowmgr-client.py",
        "bin/celery-workflowmgr",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Astronomy",
    ],
    platforms = [ "OS Independent" ],
    keywords="kat ska",
    zip_safe = False
)
