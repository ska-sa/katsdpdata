#!/usr/bin/env python
import os
import logging
from katsdpdata import FileMgrClient

from urlparse import urlparse

from optparse import OptionParser
from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler

LOG_FILENAME='/var/log/celery_workflowmgr/celery_workflowmgr.log'


class OODTWorkflowManagerException(Exception):
    pass

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
    parser.add_option('--FileMgrUrl', type='str', default='http://localhost:9101',
         help='The URL for the filemanger manager XMLRPC interface. Default is http://localhost:9101. Use http://192.186.1.50:9101 as remote file manager.')
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
        self.register_function(self.list_events, 'workflowmgr.listEvents')
        self.register_function(self.exit_event, 'exit')
        self.register_introspection_functions()
        while not self.finished:
            self.handle_request()

    def stubEvent(self, metadata):
        raise NotImplementedError

    def list_events(self):
        logging.info('List events called.')
        prefixes = ['KatFile', 'RTSTelescopeProduct', 'MeerkatTelescopeTapeProduct']
        events = [method for method in dir(self) if any([method.startswith(p) for p in prefixes])]
        for e in events:
            logging.debug('OODT Event: %s'% (e,))
        return events

    def handle_event(self, event_name, metadata):
        logging.info('Event: %s' % (event_name))
        queue = metadata['CeleryQueue'][0] if 'CeleryQueue' in metadata else self._find_queue_from_event(event_name)
        if hasattr(self, event_name):
            getattr(self, event_name)(metadata, queue)
        else:
            raise OODTWorkflowManager('No such method: %s' % (event_name))
        return True

    def exit_event(self):
        logging.info("Exit event called. Exiting.")
        self.finished = True
        return True

    def _find_queue_from_event(self, event_name):
        queue_lookup = {'KatFile': 'Kat', 'RTSTelescopeProduct': 'RTS', 'MeerkatTelescopeTapeProduct': 'Meerkat'}
        #Default queue is Kat if nothing there
        queue=[queue_lookup[prefix] for prefix in queue_lookup.keys() if event_name.startswith(prefix)]
        if not queue:
            queue= ['Kat']
        return queue[0]

class OODTWorkflowManager(WorkflowManagerXMLRPCServer):
    def __init__(self, filemgr_url, disable_backend, *args, **kwargs):
        self.filemgr_url = filemgr_url
        self.disable_backend = disable_backend
        self.filemgr = FileMgrClient(filemgr_url)
        logging.debug('Try connect to: %s.' % (filemgr_url))
        if not self.filemgr.is_alive():
            raise OODTWorkflowManagerException('Unable to connect to %s' % (self.filemgr_url))
        logging.debug('Connected to: %s.' % (filemgr_url))
        WorkflowManagerXMLRPCServer.__init__(self, *args, **kwargs)
        logging.info("Starting workflow manager on port %d." % (options.port))
        logging.info("Using file manager on %s." % (options.FileMgrUrl))

    def _get_product_info_from_filemgr(self, metadata):
        product = self.filemgr.get_product_by_name(metadata['ProductName'][0])
        data_store_ref = urlparse(product['references'][0]['dataStoreReference'])
        if os.path.split(os.path.normpath(data_store_ref.path))[1] == 'null':
            data_store_ref = urlparse(product['references'][0]['origReference'])
        product_metadata = self.filemgr.get_product_metadata(product['name'])
        logging.debug(data_store_ref)
        return data_store_ref, product_metadata

    #todo: deprecate this method in favour of RTSTelescopeProductIngest
    def RTSTelescopeProductReduce(self, metadata, queue='RTS'):
        data_store_ref, dummy_get = self._get_product_info_from_filemgr(metadata)
        #client call for this method already contains a call to the file manager
        logging.info('Filename: %s' % (metadata['Filename'][0]))
        logging.info('Reduction Name: %s' % (metadata['ReductionName'][0]))
        if self.disable_backend:
            logging.info('Disabled backend: qualification_tests.run_qualification_tests()')
        else:
            qualification_tests.run_qualification_tests(data_store_ref.path, metadata, self.filemgr_url, queue)

    #todo: deprecate this method in favour of RTSTelescopeProductIngest
    def RTSTelescopeProductRTSIngest(self, metadata, queue='RTS'):
        data_store_ref, dummy_get = self._get_product_info_from_filemgr(metadata)
        logging.info('Filename: %s' % (metadata['Filename'][0]))
        product_metadata = self.filemgr.get_product_metadata(metadata['ProductName'][0])
        if product_metadata['ReductionName'][0] == '':
            logging.info('No ReductionName. Description to ReductionName override.')
            product_metadata['ReductionName']=product_metadata['Description']
        logging.info('Reduction Name: %s' % (product_metadata['ReductionName'][0]))
        if self.disable_backend:
            logging.info('Disabled backend: No call implemented.')
        else:
            qualification_tests.run_qualification_tests(data_store_ref.path, product_metadata, self.filemgr_url, queue)

    def KatFileRTSTesting(self, metadata, queue='Kat'):
        data_store_ref, product_metadata = self._get_product_info_from_filemgr(metadata)
        logging.info('Filename: %s' % (metadata['Filename'][0]))
        logging.info('Reduction Name: %s' % (metadata['ReductionName'][0]))
        if self.disable_backend:
            logging.info('Disabled backend: qualification_tests.run_qualification_tests()')
        else:
            qualification_tests.run_qualification_tests(data_store_ref.path, product_metadata, self.filemgr_url, queue)

    def KatFileImagerPipeline(self, metadata, queue='Kat'):
        data_store_ref, product_metadata = self._get_product_info_from_filemgr(metadata)
        logging.info('Filename: %s' % (metadata['Filename'][0]))
        if self.disable_backend:
            logging.info('Disabled backend: pipelines.run_kat_cont_pipe.apply_async(queue=%s)'%queue)
        else:
            pipelines.run_kat_cont_pipe.apply_async(args=(product_metadata,),queue=queue)

    def KatFileObsReporter(self, metadata, queue='Kat'):
        data_store_ref, product_metadata = self._get_product_info_from_filemgr(metadata)
        logging.info('Filename: %s' % (metadata['Filename'][0]))
        if self.disable_backend:
            logging.info('Disabled backend: pipelines.generate_obs_report.apply_async(queue=%s)'%queue)
        else:
            pipelines.generate_obs_report.apply_async(args=(product_metadata,),queue=queue)

    def RTSTelescopeProductObsReporter(self, metadata, queue='RTS'):
        self.KatFileObsReporter(metadata)

    def MeerkatTelescopeTapeProductCheckArchiveToTape(self, metadata, queue='Meerkat'):
        #data_store_ref, product_metadata = self._get_product_info_from_filemgr(metadata)
        #logging.info('Filename: %s' % (metadata['Filename'][0]))
        logging.info('The metadata passed into this method is not passed onto the called task.')
        if self.disable_backend:
            logging.info('Disabled backend: pipelines.check_archive_to_tape.delay()')
        else:
            tasks.check_archive_to_tape.apply_async(queue=queue)

    #@todo: map PostIngestIrcInform, AutoImagerInform, ImagerPipeline, ObsReporter into all [ProductTypeName]Ingest
    #[ProductTypeName]Ingest
    def RTSTelescopeProductIngest(self, metadata, queue='RTS'):
        logging.info('Filename: %s' % (metadata['Filename'][0]))
        product_metadata = self.filemgr.get_product_metadata(metadata['ProductName'][0])
        data_store_ref, dummy_get = self._get_product_info_from_filemgr(metadata)
        #execute irc inform
        #execute qualification tests
        if product_metadata['ReductionName'][0] == '':
            logging.info('No ReductionName. Description to ReductionName override.')
            product_metadata['ReductionName']=product_metadata['Description']
        logging.info('Reduction Name: %s' % (product_metadata['ReductionName'][0]))
        if self.disable_backend:
            logging.info('Disabled backend: No call implemented.')
        else:
            qualification_tests.run_qualification_tests(data_store_ref.path, product_metadata, self.filemgr_url, queue)

    def KatFileProductIngest(self, metadata, queue='Kat'):
        logging.info('Filename: %s' % (metadata['Filename'][0]))
        product_metadata = self.filemgr.get_product_metadata(metadata['ProductName'][0])
        data_store_ref, dummy_get = self._get_product_info_from_filemgr(metadata)
        if self.disable_backend:
            logging.info('Disabled backend: No call implemented.')
        else:
            #todo: Add in IRC inform that product has been ingested
            pipelines.generate_obs_report.apply_async(args=(product_metadata,),queue=queue)
            pipelines.run_kat_cont_pipe.apply_async(args=(product_metadata,),queue=queue)

options = get_options()

if options.Foreground:
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
    logging.info('Logging to console.')
else:
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
    logging.info('Starting in daemon mode.')
    logging.info('Logging to %s' % (LOG_FILENAME))

if not options.DisableCeleryBackend:
    #then import the backend
    from katsdpworkflow.RTS import qualification_tests
    from katsdpworkflow.KAT7 import pipelines
    from katsdpworkflow.MKAT import tasks

server = OODTWorkflowManager(options.FileMgrUrl, options.DisableCeleryBackend, ("", options.port,))
if options.DisableCeleryBackend:
    logging.info("No tasks will be pushed onto the celery backend.")
server.serve_forever()
