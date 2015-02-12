from distutils.core import setup, Extension
import os, sys, glob

__version__ = '0.0.1'

setup(name = 'tape_interface',
    version = __version__,
    description = 'Interface to the Oracle SL150 tape store web manager',
    long_description = 'Exposes functionality available to the web manager to python and katcp',
    license = 'GPL',
    author = 'Christopher Schollar',
    author_email = 'ctgschollar at gmail.com',
    classifiers=[
        'Development Status :: 1 - Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Topic :: Scientific/Engineering :: Astronomy',
        'Topic :: Software Development :: Libraries :: Python Modules',
        ],
    provides=['tape_interface'],
    package_dir = {'tape_interface':'src'},
    packages = ['tape_interface'])
