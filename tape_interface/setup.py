#!/usr/bin/env python
from setuptools import setup, find_packages

__version__ = '0.0.1'

setup(name = 'katsdptape',
    version = __version__,
    description = 'Interface to the Oracle SL150 tape library',
    long_description = 'Exposes tape functionality to katcp',
    license = 'GPL',
    author = 'Christopher Schollar',
    author_email = 'cschollar@ska.ac.za',
    url = 'http://ska.ac.za/',
    scripts = [
       "scripts/vis_store_controller.py"
    ],
     classifiers=[
        'Development Status :: 1 - Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Topic :: Scientific/Engineering :: Astronomy',
        'Topic :: Software Development :: Libraries :: Python Modules',
        ],
    platforms = [ "Linux" ],
    keywords="kat ska",
    zip_safe = False
)
