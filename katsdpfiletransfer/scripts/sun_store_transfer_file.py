#!/usr/bin/env python
from optparse import OptionParser
import logging
import os
import katsdpfiletransfer

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
    parser.add_option('--calc-md5', action='store_true', default=False,
        help='Calculate the md5 checksum and transfer to the sunstore.')
    parser.add_option('--delete', action='store_true', default=False,
        help='Delete the file after successful transfer to the sunstore.')
    parser.add_option('--debug', action='store_true', default=False,
        help='Set logging level to debug.')
    (options, args) = parser.parse_args()
    return options

opts = get_options()
logging.basicConfig(level=logging.DEBUG if opts.debug else logging.INFO, format='%(asctime)s %(levelname)s %(message)s', handlers=[logging.StreamHandler()])                
if opts.on_success and not os.path.isdir(opts.on_success):
    os.makedirs(opts.on_success)
transferer = katsdpfiletransfer.SunStoreTransfer(local_path=opts.path, on_success_path=opts.on_success, regex=opts.regex, period=opts.sleep)
transferer.run()
