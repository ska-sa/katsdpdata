#!/usr/bin/env python
import os
import logging
from katsdpdata import FileMgrClient

from urlparse import urlparse

from optparse import OptionParser
from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler

LOG_FILENAME='/var/log/celery_workflowmgr/celery_workflowmgr.log'

def get_options():
    """Sets options from the arguments passed to the script.

    Returns
    -------
    options: (Boolean, optparse.Values)
        Options and arguments.
    """

    usage = 'usage: %prog [options]'

    parser = OptionParser(usage=usage)
    parser.add_option('-p', '--port', type='int',
         help='The port to listen on for XMLRPC requests')
    parser.add_option('--Foreground', action='store_true', default=False,
         help='For testing purposes. Log to console and not the file.')
    parser.add_option('--DisableCeleryBackend', action='store_true', default=False,
         help='For testing purposes. Disable push onto the celery queue.')

    (options, args) = parser.parse_args()

    return options

class WorkflowManagerXMLRPCServer(SimpleXMLRPCServer):
    def __init__(self, *args, **kwargs):
        SimpleXMLRPCServer.__init__(self, requestHandler=SimpleXMLRPCRequestHandler, *args, **kwargs)

    def serve_forever(self):
        self.finished = False
        self.register_function(self.handle_event, 'workflowmgr.handleEvent')
        self.register_function(self.exit_event, 'exit')
        self.register_introspection_functions()
        while not self.finished:
            self.handle_request()

    def stubEvent(self, metadata):
        raise NotImplementedError

    def handle_event(self, event_name, metadata):
        logging.info('Event: %s' % (event_name))
        getattr(self, event_name)(metadata)
        return True

    def exit_event(self):
        logging.info("Exit event called. Exiting.")
        self.finished = True
        return True

class OODTWorkflowManager(WorkflowManagerXMLRPCServer):
    def __init__(self, filemgr_url, disable_backend, *args, **kwargs):
        self.filemgr_url = filemgr_url
        self.disable_backend = disable_backend
        self.filemgr = FileMgrClient(filemgr_url)
        WorkflowManagerXMLRPCServer.__init__(self, *args, **kwargs)

    def _get_product_info_from_filemgr(self, metadata):
        product = self.filemgr.get_product_by_name(metadata['ProductName'][0])
        data_store_ref = urlparse(product['references'][0]['dataStoreReference'])
        if os.path.split(os.path.normpath(data_store_ref.path))[1] == 'null':
            data_store_ref = urlparse(product['references'][0]['origReference'])
        product_metadata = self.filemgr.get_product_metadata(product['name'])
        return data_store_ref, product_metadata

    def RTSTelescopeProductReduce(self, metadata):
        data_store_ref, dummy_get = self._get_product_info_from_filemgr(metadata)
        #client call for this method already  contains a call to the file manager
        logging.info('Filename: %s' % (metadata['Filename'][0]))
        logging.info('Reduction Name: %s' % (metadata['ReductionName'][0]))
        if self.disable_backend:
            logging.info('Disabled backend: qualification_tests.run_qualification_tests()')
        else:
            qualification_tests.run_qualification_tests(data_store_ref.path, metadata, self.filemgr_url)

    def RTSTelescopeProductRTSIngest(self, metadata):
        logging.info('Filename: %s' % (metadata['Filename'][0]))
        logging.info('Reduction Name: %s' % (metadata['ReductionName'][0]))

    def KatFileRTSTesting(self, metadata):
        data_store_ref, product_metadata = self._get_product_info_from_filemgr(metadata)
        logging.info('Filename: %s' % (metadata['Filename'][0]))
        logging.info('Reduction Name: %s' % (metadata['ReductionName'][0]))
        if self.disable_backend:
            logging.info('Disabled backend: qualification_tests.run_qualification_tests()')
        else:
            qualification_tests.run_qualification_tests(data_store_ref.path, product_metadata, self.filemgr_url)

    def KatFileImagerPipeline(self, metadata):
        data_store_ref, product_metadata = self._get_product_info_from_filemgr(metadata)
        logging.info('Filename: %s' % (metadata['Filename'][0]))
        if self.disable_backend:
            logging.info('Disabled backend: pipelines.run_kat_cont_pipe.delay()')
        else:
            pipelines.run_kat_cont_pipe.delay(product_metadata)

    def KatFileObsReporter(self, metadata):
        data_store_ref, product_metadata = self._get_product_info_from_filemgr(metadata)
        logging.info('Filename: %s' % (metadata['Filename'][0]))
        if self.disable_backend:
            logging.info('Disabled backend: pipelines.generate_obs_report.delay()')
        else:
            pipelines.generate_obs_report.delay(product_metadata)

    def MeerkatTelescopeTapeProductCheckArchiveToTape(self, metadata):
        data_store_ref, product_metadata = self._get_product_info_from_filemgr(metadata)
        logging.info('Filename: %s' % (metadata['Filename'][0]))
        if self.disable_backend:
            logging.info('Disabled backend: pipelines.check_archive_to_tape.delay()')
        else:
            pipelines.check_archive_to_tape.delay(product_metadata)

options = get_options()

if not options.DisableCeleryBackend:
    #then import the backend
    from katsdpworkflow.RTS import qualification_tests
    from katsdpworkflow.KAT7 import pipelines
    from katsdpworkflow.MEERKAT import tasks

if options.Foreground:
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
    logging.info('Logging to console')
else:
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO)
    logging.info('Starting in daemon mode')

server = OODTWorkflowManager('http://localhost:9101', options.DisableCeleryBackend, ("", options.port,))
logging.info("Starting workflow manager on port %d" % (options.port))
logging.info("Using file manager on http://localhost:9101")
if options.DisableCeleryBackend:
    logging.info("No tasks will be pushed onto the celery backend")
server.serve_forever()
