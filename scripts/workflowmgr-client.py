#!/usr/bin/env python
import xmlrpclib
import katsdpdata

from optparse import OptionParser

def get_options():
    """Sets options from the arguments passed to the script.

    Returns
    -------
    options: (Boolean, optparse.Values)
        Options and arguments.
    """

    usage = 'usage: %prog [options]'

    parser = OptionParser(usage=usage)
    parser.add_option('--url', type='str',
         help='The URL for the workflow manager XMLRPC interface.')
    parser.add_option('--filename', type='str',
         help='The file to use to fake workflow metatdata')
    parser.add_option('--KatFileObsReporter', action='store_true', default=False,
         help='Call KatFileObsReporter on the xmlrpc interface.')
    parser.add_option('--KatFileImagerPipeline', action='store_true', default=False,
         help='Call KatFileObsReporter on the xmlrpc interface.')
    parser.add_option('--CallExit', action='store_true', default=False,
         help='Call exit on the xmlrpc interface')
    parser.add_option('--TestTuonarePipeline', action='store_true', default=False,
         help='Call TestTuonarePipeline on the xmlrpc interface.')
    parser.add_option('--KatFileRTSTesting', action='store_true', default=False,
         help='Call KatFileRTSTesting on the xmlrpc interface.')
    (options, args) = parser.parse_args()

    return options

#initialise
opts = get_options()

xmlrpc_client = xmlrpclib.ServerProxy(opts.url)

product_metadata = None

if opts.filename:
    fm = katsdpdata.FileMgrClient('http://192.168.1.50:9101')
    product_metadata = fm.get_product_metadata(opts.filename)

if opts.KatFileObsReporter and product_metadata:
    xmlrpc_client.workflowmgr.handleEvent('KatFileObsReporter', product_metadata)
if opts.KatFileImagerPipeline and product_metadata:
    xmlrpc_client.workflowmgr.handleEvent('KatFileImagerPipeline', product_metadata)
if opts.TestTuonarePipeline and product_metadata:
    xmlrpc_client.workflowmgr.handleEvent('TestTuonarePipeline', product_metadata)
if opts.KatFileRTSTesting and product_metadata:
    xmlrpc_client.workflowmgr.handleEvent('KatFileRTSTesting', product_metadata)
if opts.CallExit:
    while True:
        user_input = raw_input("Shutdown the workflow manager? [Y/N]: ").strip().upper()
        if user_input.strip().upper() in ['Y', 'N']:
            break
    if user_input == 'Y':
        xmlrpc_client.exit()
