#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name = "katsdpdata",
    description = "Data search and retrieval library for the MeerKAT project",
    author = "Thomas Bennett, Chris Schollar",
    author_email = "thomas@ska.ac.za, cschollar@ska.ac.za",
    packages = find_packages(),
    setup_requires = ["katversion"],
    install_requires = [
        "pysolr>=3.2.0"#,
        #"MySQL-python"
    ],
    url = 'http://ska.ac.za/',
    scripts = [
        "scripts/tel_prod_met_extractor.py",
        "scripts/red_prod_met_extractor.py",
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
    zip_safe = False,
    use_katversion = True
)
