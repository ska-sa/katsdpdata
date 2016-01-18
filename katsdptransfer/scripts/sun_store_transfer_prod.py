#!/usr/bin/env python
from optparse import OptionParser
import logging
import os
import katsdptransfer

def get_options():
    """Sets options from the arguments passed to the script.
    Returns
    -------
    options: (Boolean, optparse.Values)
        Options and arguments.
    """
    usage = 'usage: %prog [options]'
    parser = OptionParser(usage=usage)
    parser.add_option('--filename', type='str',
        help='The name of the file to transfer.')
    parser.add_option('--debug', action='store_true', default=False,
        help='Set logging level to debug.')    (options, args) = parser.parse_args()
    return options

opts = get_options()
logging.basicConfig(level=logging.DEBUG if opts.debug else logging.INFO, format='%(asctime)s %(levelname)s %(message)s', handlers=[logging.StreamHandler()])                
transferer = katsdptransfer.SunStoreTransferFile(filename=opts.filename)
transferer.run()
