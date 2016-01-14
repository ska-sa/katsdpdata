#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name = "katsdpfiletransfer",
    description = "Data transfer library for the MeerKAT archiving",
    author = "Chris Schollar, Thomas Bennett",
    author_email = "cschollar@ska.ac.za, thomas@ska.ac.za",
    packages = find_packages(),
    url = 'http://ska.ac.za/',
    scripts = [
       "scripts/sun_store_transfer_daemon.py",
       "scripts/sun_store_transfer_file.py",
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
