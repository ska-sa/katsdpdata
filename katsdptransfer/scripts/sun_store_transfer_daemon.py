#!/usr/bin/env python
from optparse import OptionParser
import logging
import os
import katsdpprodtf

def get_options():
    """Sets options from the arguments passed to the script.
    Returns
    -------
    options: (Boolean, optparse.Values)
        Options and arguments.
    """
    usage = 'usage: %prog [options]'
    parser = OptionParser(usage=usage)
    parser.add_option('--path', type='str', default='/var/kat/data/staging/',
        help='The direct to check for files to transfere. Default is "/var/kat/data/staging/".')
    parser.add_option('--regex', type='str', default='[0-9]{10}\.h5$',
        help='File name regular expression filter. Deafult is "[0-9]{10}\.h5$".')
    parser.add_option('--sleep', type='int', default=10,
        help='Sleep time in seconds before checking path for new files.')
    parser.add_option('--on_success', type='str',
        help='On a sucessful transfer move the file to this directory. If not set files will be deleted.')
    parser.add_option('--debug', action='store_true', default=False,
        help='Set logging level to debug.')
    (options, args) = parser.parse_args()
    return options

opts = get_options()

logging.basicConfig(level=logging.DEBUG if opts.debug else logging.INFO, format='%(asctime)s %(levelname)s %(message)s', handlers=[logging.StreamHandler()])                
if opts.on_success and not os.path.isdir(opts.on_success):
    os.makedirs(opts.on_success)
transferer = katsdpprodtf.SunStoreTransferDaemon(local_path=opts.path, on_success_path=opts.on_success, regex=opts.regex, period=opts.sleep)
transferer.run()
