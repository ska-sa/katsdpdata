#!/usr/bin/env python
import katsdpdata
import logging
import xmlrpclib

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
    parser.add_option('--url', type='str', default='http://192.168.6.185:9103',
         help='The URL for the workflow manager XMLRPC interface. Default is http://192.168.6.185:9103')
    parser.add_option('--FileMgrUrl', type='str', default='http://192.168.1.50:9101',
         help='The URL for the file manager XMLRPC interface. Default is http://192.168.1.50:9101.')
    parser.add_option('--filename', type='str',
         help='The file to reduce')
    parser.add_option('--KatFileObsReporter', action='store_true', default=False,
         help='Call KatFileObsReporter on the xmlrpc interface.')
    parser.add_option('--RTSTelescopeProductObsReporter', action='store_true', default=False,
         help='Call RTSTelescopeProduct on the xmlrpc interface.')
    parser.add_option('--KatFileImagerPipeline', action='store_true', default=False,
         help='Call KatFileImagerPipeline on the xmlrpc interface.')
    parser.add_option('--KatFileRTSTesting', action='store_true', default=False,
         help='Call KatFileRTSTesting on the xmlrpc interface.')
    parser.add_option('--MeerkatTelescopeTapeProductCheckArchiveToTape', action='store_true', default=False,
         help='Call MeerkatTelescopeTapeProductCheckArchiveToTape on the xmlrpc interface.')
    parser.add_option('--RTSTelescopeProductReduce', action='store_true', default=False,
         help='Call RTSTelescopeProductReduce. Note you need to specify the reduction to perform.')
    parser.add_option('--ReductionName', type='str',
         help='String containing the data to set for ReductionName metadata')
    parser.add_option('--ListEvents', action='store_true', default=False,
         help='List OODT events.')
    parser.add_option('--CallExit', action='store_true', default=False,
         help='Call exit on the xmlrpc interface')
    (options, args) = parser.parse_args()

    return options

#initialise
opts = get_options()
logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
xmlrpc_client = xmlrpclib.ServerProxy(opts.url)

product_metadata = None
if opts.filename:
    fm = katsdpdata.FileMgrClient(opts.FileMgrUrl)
    product_metadata = fm.get_product_metadata(opts.filename)

if not product_metadata:
    logging.warning('No product metadata specified.')

if opts.KatFileObsReporter and product_metadata:
    product_metadata['CeleryQueue'] = ['manual']
    logging.info('Calling handleEvent KatFileObsReporter with product %s' % (opts.filename))
    xmlrpc_client.workflowmgr.handleEvent('KatFileObsReporter', product_metadata)

if opts.RTSTelescopeProductObsReporter and product_metadata:
    product_metadata['CeleryQueue'] = ['manual']
    logging.info('Calling handleEvent RTSTelescopeProductObsReporter with product %s' % (opts.filename))
    xmlrpc_client.workflowmgr.handleEvent('RTSTelescopeProductObsReporter', product_metadata)

if opts.KatFileImagerPipeline and product_metadata:
    product_metadata['CeleryQueue'] = ['manual']
    logging.info('Calling handleEvent KatFileImagerPipeline with product %s' % (opts.filename))
    xmlrpc_client.workflowmgr.handleEvent('KatFileImagerPipeline', product_metadata)

if opts.KatFileRTSTesting and product_metadata:
    product_metadata['CeleryQueue'] = ['manual']
    product_metadata['ReductionName'] = product_metadata['Description']
    logging.info('Calling handleEvent KatFileRTSTesting with product %s with reductions %s' % (product_metadata['ReductionName'][0]))
    xmlrpc_client.workflowmgr.handleEvent('KatFileRTSTesting', product_metadata)

if opts.MeerkatTelescopeTapeProductCheckArchiveToTape and product_metadata:
    product_metadata['CeleryQueue'] = ['manual']
    product_metadata['ReductionName'] = product_metadata['Description']
    logging.info('Calling handleEvent KatFileRTSTesting with product %s with reductions %s' % (product_metadata['ReductionName'][0]))
    xmlrpc_client.workflowmgr.handleEvent('MeerkatTelescopeTapeProductCheckArchiveToTape', product_metadata)

if opts.RTSTelescopeProductReduce and product_metadata and opts.ReductionName:
    product_metadata['CeleryQueue'] = ['manual']
    product_metadata['ReductionName'] = [opts.ReductionName]
    logging.info('Calling handleEvent RTSTelescopeProductReduce with product %s with reductions %s' % (product_metadata['ReductionName'][0]))
    xmlrpc_client.workflowmgr.handleEvent('RTSTelescopeProductReduce', product_metadata)

if opts.ListEvents:
    for e in xmlrpc_client.workflowmgr.listEvents():
        logging.info('OODT Event: %s' % e)

if opts.CallExit:
    while True:
        user_input = raw_input("Shutdown the workflow manager? [Y/N]: ").strip().upper()
        if user_input.strip().upper() in ['Y', 'N']:
            break
    if user_input == 'Y':
        logging.info('Calling exit.')
        xmlrpc_client.exit()
