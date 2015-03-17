import os
import subprocess
import time

import katdal

from katcp import BlockingClient, Message
from xml.etree import ElementTree

class MetExtractorException(Exception):
    """Raises a MetExtractor exception."""
    pass

class MetExtractor(object):
    """Base class for handling metadata extraction. This class can be used to
    create an empty met file that complies with OODT ingest"

    Attributes
    ----------
    metadata : dict
        Holds key-value pairs for metadata.
    """
    def __init__(self, metadata_filename):
        super(MetExtractor, self).__init__()
        self.metadata = {}
        self.product_type = None
        self.metadata_filename = metadata_filename
        self._metadata_extracted = False

    def extract_metadata(self):
        raise NotImplementedError

    def __str__(self):
        xml_tree = ElementTree.Element('cas:metadata')
        xml_tree.set('xmlns:cas', 'http://oodt.jpl.nasa.gov/1.0/cas')
        for k in self.metadata.keys():
            keyval = ElementTree.SubElement(xml_tree, 'keyval')
            key = ElementTree.SubElement(keyval, 'key')
            key.text = str(k)
            if isinstance(self.metadata[k], list):
                for text in self.metadata[k]:
                    val = ElementTree.SubElement(keyval, 'val')
                    val.text = text
            else:
                val = ElementTree.SubElement(keyval, 'val')
                val.text = self.metadata[k]
        return ElementTree.tostring(xml_tree, 'utf-8')

    def write_metadatafile(self):
        if self._metadata_extracted:
            with open(self.metadata_filename, 'w') as metfile:
                metfile.write(str(self))
        else:
            raise MetExtractorException('No metadata extracted.')

class Kat7TelescopeProductMetExtractor(MetExtractor):
    """Used for extracting metdata from an HDF5 KAT File.

    Parameters
    ----------
    katdata : class : DataSet
        The access handler to a HDF5 katdata.

    Attributes
    ----------
    katdata : class : DataSet
        The access handler to a HDF5 katdata.
    """
    def __init__(self, filename):
        self.filename = os.path.abspath(filename)
        super(Kat7TelescopeProductMetExtractor, self).__init__('%s.%s' % (self.filename, 'met',))
        self._katdata = katdal.open(os.path.abspath(filename))
        self.product_type = 'Katfile' #TODO - modify to 'Kat7TelescopeProduct'

    def _extract_metadata_product_type(self):
        self.metadata['ProductType'] = self.product_type

    def _extract_metadata_from_katdata(self):
        """Populate self.metadata with information scraped from katfile"""
        self.metadata['Antennas'] = [a.name for a in self._katdata.ants]
        self.metadata['CenterFrequency'] = str(self._katdata.channel_freqs[self._katdata.channels[-1]/2])
        self.metadata['ChannelWidth'] = str(self._katdata.channel_width)
        self.metadata['Description'] = self._katdata.description
        self.metadata['Details'] = str(self._katdata)
        self.metadata['DumpPeriod'] = '%.4f' % (self._katdata.dump_period)
        self.metadata['Duration'] = str(round(self._katdata.end_time-self._katdata.start_time, 2))
        self.metadata['ExperimentID'] = self._katdata.experiment_id
        self.metadata['FileSize'] = str(os.path.getsize(self._katdata.file.filename))
        self.metadata['InstructionSet'] = '%s %s' % (self._katdata.obs_params['script_name'], self._katdata.obs_params['script_arguments'])
        self.metadata['KatfileVersion'] = self._katdata.version
        self.metadata['KatpointTargets'] = list(set([str(i).replace('tags=','') for i in self._katdata.catalogue.targets if i.name not in ['None', 'Nothing']]))
        self.metadata['NumFreqChannels'] = str(len(self._katdata.channels))
        self.metadata['Observer'] = self._katdata.observer
        self.metadata['RefAntenna'] = self._katdata.ref_ant
        self.metadata['StartTime'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(self._katdata.start_time))
        self.metadata['Targets'] = list(set([i.name for i in self._katdata.catalogue.targets if i.name not in ['None', 'Nothing', 'azel', 'radec']]))

    def _extract_metadata_file_digest(self):
        print 'Calculating the md5 checksum for %s. This may take a while.' % (self.filename)
        p = subprocess.Popen(['md5sum', self.filename], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        if not p[1]:
            self.metadata['FileDigest'] = p[0].split()[0]
        print 'md5 checksum complete. Digest is %s.' % self.metadata['FileDigest']
    
    def extract_metadata(self):
        self._extract_metadata_product_type()
        self._extract_metadata_from_katdata()
        self._extract_metadata_file_digest()
        self._metadata_extracted = True

class RTSTelescopeProductMetExtractor(Kat7TelescopeProductMetExtractor):
    """Used for extracting metdata from an HDF5 KAT File that has been captured by the RTS.
    Parameters
    ----------
    katdata : class : DataSet
        The access handler to a HDF5 katdata.

    Attributes
    ----------
    katdata : class : DataSet
        The access handler to a HDF5 katdata.
    """
    def __init__(self, katfile, metadata_key = 'ReductionName', obs_param = 'reduction_name'):
        super(RTSTelescopeProductMetExtractor, self).__init__(katfile)
        self._metadata_key = 'ReductionName'
        self._obs_param = 'reduction_name'
        #always set product_type after call to super
        self.product_type = 'RTSTelescopeProduct'

    def _extract_metadata_from_katdata(self):
        """Populate self.metadata with information scraped from self._katdata"""
        super(RTSTelescopeProductMetExtractor, self)._extract_metadata_from_katdata()
        self.metadata[self._metadata_key] = self._katdata.obs_params.get(self._obs_param, '')

class MeerkatTelescopeTapeProductMetExtractor(Kat7TelescopeProductMetExtractor):
    """Used for extracting metdata from a katcp sensor.
    Parameters
    ----------
    katfile : string : name of the katfile to open.
        Fileame for access handler to a HDF5 katdata.

    Attributes
    ----------
    katdata : class : DataSet
        The access handler to a HDF5 katdata.
    """
    def __init__(self, katfile, server_host='192.168.6.233', server_port=5001):
        super(MeerkatTelescopeTapeProductMetExtractor, self).__init__(katfile)
        self.server_host = server_host
        self.server_port = server_port
        self.metadata_key = 'TapeBufferDirectory'
        self.sensor_name = 'buffer_dir'
        #always set product_type after call to super
        self.product_type = 'MeerkatTelescopeTapeProduct'

    def _extract_metadata_from_katsensor(self):
        """Populate self.metadata with information from a katcp request"""
        client = BlockingClient(self.server_host, self.server_port)
        client.start()
        client.wait_protocol()
        reply, informs = client.blocking_request(Message.request("sensor-value", self.sensor_name))
        client.stop()
        client.join()
        self.metadata[self.metadata_key] = informs[0].arguments[-1]

    def extract_metadata(self):
        self._extract_metadata_from_katsensor()
        super(MeerkatTelescopeTapeProductMetExtractor, self).extract_metadata()
