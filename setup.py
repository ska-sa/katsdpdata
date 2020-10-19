#!/usr/bin/env python3
from setuptools import setup, find_packages

setup(
    name="katsdpdata",
    description="Data search and retrieval library for the MeerKAT project",
    author="Thomas Bennett, Chris Schollar",
    author_email="thomas@ska.ac.za, cschollar@ska.ac.za",
    packages=find_packages(),
    setup_requires=["katversion"],
    install_requires=[
        "boto==2.48.0", "katdal", "katpoint", "katsdpservices",
        "katsdptelstate", "numpy", "pysolr>=3.3.3"],
    url='http://ska.ac.za/',
    scripts=[
        "scripts/tel_prod_met_extractor.py",
        "scripts/download_cbid_prods.py",
        "scripts/download_cbid_prods_maximum_plaid.py",
        "scripts/vis_trawler.py"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Astronomy"],
    platforms=["OS Independent"],
    keywords="meerkat ska",
    zip_safe=False,
    use_katversion=True
)
