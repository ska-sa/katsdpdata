#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name = "katsdptape",
    description = "Tape Library interface for the MeerKAT project",
    author = "Chris Schollar",
    author_email = "cschollar@ska.ac.za",
    packages = find_packages(),
    url = 'http://ska.ac.za/',
    scripts = [
        "scripts/vis_store_controller.py"
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
