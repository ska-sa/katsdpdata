#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name = "katsdpdata",
    description = "Data search and retrieval library for the MeerKAT project",
    author = "Thomas Bennett",
    author_email = "thomas@ska.ac.za",
    packages = find_packages(),
    install_requires = [
        "pysolr"
    ],
    url = 'http://ska.ac.za/',
    scripts = [
        "scripts/katfile_met_extractor.py",
        "scripts/rts_met_extractor.py",
        "scripts/mkat_tape_met_extractor.py",
        "scripts/workflowmgr-client.py",
        "scripts/celery-workflowmgr.py",
        "bin/cas-crawler-client.sh",
        "bin/cas-filemgr-client.sh",
        "bin/celery-workflowmgr"
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
