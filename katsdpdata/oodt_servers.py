from optparse import OptionParser
from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler

class WorkflowManagerXMLRPCServer(SimpleXMLRPCServer):
    def __init__(self, *args, **kwargs):
        SimpleXMLRPCServer.__init__(self, requestHandler=SimpleXMLRPCRequestHandler, *args, **kwargs)
        self.celery_results = []

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
        getattr(self, event_name)(metadata)
        return True

    def exit_event(self):
        self.finished = True
        return True

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

    (options, args) = parser.parse_args()

    return options
