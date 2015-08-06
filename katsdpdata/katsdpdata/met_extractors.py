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

    Parameters
    ----------
    metadata_filename : string : name of the metadata output file to use.
        Filename for access handler to CAS metadata file.

    Attributes
    ----------
    metadata_filename : string : Name of the metadata file to create.
    
    metadata : dict : Place holder for metadata key value pairs.
    
    product_type : string : Specify product type for OODT Filemananger ingest
        set to None

    Hidden Attributes
    -----------------
    _metadata_extracted : boolean : keep track of metadata extraction.
        The access handler to a katfile.
    """

    def __init__(self, metadata_filename):
        super(MetExtractor, self).__init__()
        self.metadata_filename = metadata_filename
        self.metadata = {}
        self.product_type = None
        self._metadata_extracted = False

    def _extract_metadata_product_type(self):
        self.metadata['ProductType'] = self.product_type

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

    def extract_metadata(self):
        raise NotImplementedError

    def write_metadatafile(self):
        if self._metadata_extracted:
            with open(self.metadata_filename, 'w') as metfile:
                metfile.write(str(self))
        else:
            raise MetExtractorException('No metadata extracted.')

class TelescopeProductMetExtractor(MetExtractor):
    """Base class for handling telescope systems metadata extraction. This class contains
    a factory which returns the correct met extractor object to use. The following systems
    are currently supported: KAT7, RTS, MeerKAT AR1.

    Parameters
    ----------
    metadata_filename : string : name of the metadata output file to use.
        Filename for access handler to CAS metadata file.

    Attributes
    ----------
    metadata_filename : string : Name of the metadata file to create.
    
    metadata : dict : Place holder for metadata key value pairs.
    
    product_type : string : Specify product type for OODT Filemananger ingest
        set to None
    """
    def __init__(self, katdata):
        self._katdata = katdata
        self.katfile = os.path.abspath(self._katdata.file.filename)
        super(TelescopeProductMetExtractor, self).__init__('%s.%s' % (self.katfile, 'met',))

    def _extract_metadata_from_katdata(self):
        """Populate self.metadata: Get information using katdal"""
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
        """Populate self.metadata: Calculate the md5 checksum and create a digest metadata key"""
        print 'Calculating the md5 checksum for %s. This may take a while.' % (self.katfile)
        p = subprocess.Popen(['md5sum', self.katfile], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        if not p[1]:
            self.metadata['FileDigest'] = p[0].split()[0]
        print 'md5 checksum complete. Digest is %s.' % self.metadata['FileDigest']

    @staticmethod
    def factory(katfile):
        """Static method to instantiate the correct metadata extraction object. The following systems
        are currently supported: KAT7, RTS, MeerKAT AR1.
        
        Parameters:
        ----------
        katfile: string : name of file to opened with the katdal module.
        """
        katdata = katdal.open(katfile)
        try:
            telescope_id = katdata.file['TelescopeState'].attrs['subarray_product_id']
            if telescope_id == 'rts_c856M4k':
                return MeerKATAR1TelescopeProductMetExtractor(katdata)
            elif katdata.sub_array_product_id == 'RTS':
                return RTSTelescopeProductMetExtractor(katdata)
            elif katdata.sub_array_product_id == 'KAT7':
                return KAT7TelescopeProductMetExtractor(katdata)
            else:
                raise MetExtractorException('Bad met extractor creation.')
        except KeyError:
            if katdata.ants[0].name.startswith('ant'):
                #must be KAT7
                return KAT7TelescopeProductMetExtractor(katdata)
            else:
                #must be RTS
                return RTSTelescopeProductMetExtractor(katdata)

class KAT7TelescopeProductMetExtractor(TelescopeProductMetExtractor):
    """Used for extracting metdata for a KAT7 Telescope product. As well as extracting data from a
    katfile, a further metadata key 'ReductionName' might be present. Set it if it is, otherwise
    empty string.

    Parameters
    ----------
    katdata : class : katdal object
        Katdal access handler.

    Attributes
    ----------
    katfile : string : name of file for current metadata extraction
    
    product_type : string : Specify product type for OODT Filemananger ingest
        set to 'KAT7TelescopeProduct'
    metadata_filename : string : Name of the metadata file to create.
    
    metadata : dict : Place holder for metadata key value pairs.
    
    product_type : string : Specify product type for OODT Filemananger ingest
    """

    def __init__(self, katdata):
        super(KAT7TelescopeProductMetExtractor, self).__init__(katdata)
        self.product_type = 'KAT7TelescopeProduct'

    def _extract_metadata_for_project(self):
        """Extract metadata key value pairs from the command line stored in the katdal file."""
        pass

    def extract_metadata(self):
        """Metadata to extract for this product. Test value of self.__metadata_extracted. If 
        True, this method has already been run once. If False, extract metadata.
        This includes:
            * extracting the product type
            * extracting basic hdf5 information
            * extacting project related information
            * extracting an md5 checksum
        """
        if not self._metadata_extracted:
            self._extract_metadata_product_type()
            self._extract_metadata_from_katdata()
            self._extract_metadata_for_project()
            self._extract_metadata_file_digest()
            self._metadata_extracted = True
        else:
            print "Metadata already extracted. Set the metadata_extracted attribute to False and run again."

class RTSTelescopeProductMetExtractor(TelescopeProductMetExtractor):
    """Used for extracting metdata for a RTSTelescopeProduct. As well as extracting data from a
    katdal, a further metadata key 'ReductionName' is created.

    Parameters
    ----------
    katdata : class : katdal object
        Katdal access handler.

    Attributes
    ----------
    katfile : string : name of file for current metadata extraction
    
    product_type : string : Specify product type for OODT Filemananger ingest
        set to 'RTSTelescopeProduct'
    metadata_filename : string : Name of the metadata file to create.
    
    metadata : dict : Place holder for metadata key value pairs.
    
    product_type : string : Specify product type for OODT Filemananger ingest
    """

    def __init__(self, katdata):
        super(RTSTelescopeProductMetExtractor, self).__init__(katdata)
        #always set product_type after call to super
        self.product_type = 'RTSTelescopeProduct'

    def _extract_metadata_for_auto_reduction(self):
        """Populate self.metadata with information scraped from self"""
        metadata_key_to_map = 'ReductionName'
        obs_param_to_get = 'reduction_name'
        self.metadata[metadata_key_to_map] = self._katdata.obs_params.get(obs_param_to_get, '')

    def extract_metadata(self):
        """Metadata to extract for this product. Test value of self.__metadata_extracted. If 
        True, this method has already been run once. If False, extract metadata.
        This includes:
            * extracting the product type
            * extracting basic hdf5 information
            * extracting auto reduction
            * extracting an md5 checksum
        """
        if not self._metadata_extracted:
            self._extract_metadata_product_type()
            self._extract_metadata_from_katdata()
            self._extract_metadata_for_auto_reduction()
            self._extract_metadata_file_digest()
            self._metadata_extracted = True
        else:
            print "Metadata already extracted. Set the metadata_extracted attribute to False and run again."

class MeerKATAR1TelescopeProductMetExtractor(TelescopeProductMetExtractor):
    """Used for extracting metdata for a MeerKATAR1TelescopeProduct.

    Parameters
    ----------
    katdata : class : katdal object
        Katdal access handler

    Attributes
    ----------
    katfile : string : name of file for current metadata extraction
    
    product_type : string : Specify product type for OODT Filemananger ingest
        set to 'MeerKATAR1TelescopeProduct'
    metadata_filename : string : Name of the metadata file to create.
    
    metadata : dict : Place holder for metadata key value pairs.
    
    product_type : string : Specify product type for OODT Filemananger ingest
    """
 
    def __init__(self, katdata):
        super(MeerKATAR1TelescopeProductMetExtractor, self).__init__(katdata)
        #override product_type
        self.product_type = 'MeerKATAR1TelescopeProduct'

    def _extract_sub_array_product_id(self):
        self.metadata['SubarrayProductId'] = self._katdata.file['TelescopeState'].attrs['subarray_product_id']

    def extract_metadata(self):
        """Metadata to extract for this product. Test value of self.__metadata_extracted. If 
        True, this method has already been run once. If False, extract metadata.
        This includes:
            * extracting the product type
            * extracting basic hdf5 information
            * extracting the sub array product id
            * extracting an md5 checksum
        """
        if not self._metadata_extracted:
            self._extract_metadata_product_type()
            self._extract_sub_array_product_id()
            self._extract_metadata_from_katdata()
            self._extract_metadata_file_digest()
            self._metadata_extracted = True
        else:
            print "Metadata already extracted. Set the metadata_extracted attribute to False and run again."

class MeerkatTelescopeTapeProductMetExtractor(TelescopeProductMetExtractor):
    """Used for extracting metdata for a MeerkatTelescopeTapeProduct. As well as extracting data
    from a katfile, a further metadata key 'TapeBufferDirectory' must be gotten from a katcp server
    sensor.

    Parameters
    ----------
    katfile : string : name of the katfile to open.
        Filename for access handler to a HDF5 katdata.

    Attributes
    ----------
    product_type : string : Specify product type for OODT Filemananger ingest
        set to 'MeerkatTelescopeTapeProduct'

    Hidden Attributes
    -----------------
    _sensor_host : string : ip address of the vis-store-tape
        set to '192.168.6.233'
    _katcp_port : integer : katcp port
        set to 5001
    _metadata_key_to_map : string : The extra key to map
        set to 'TapeBufferDirectory'
    _sensor_name_to_get : string : katcp sensor name
        'buffer_dir'
    _repository_path : string : the repository path
        The OODT product repository path. Default is '/var/kat/data'
    """

    def __init__(self, katdata):
        super(MeerkatTelescopeTapeProductMetExtractor, self).__init__(katdata)
        self._sensor_host = '192.168.6.233'
        self._katcp_port = 5001
        self._repository_path='/var/kat/data'
        #always set product_type after call to super
        self.product_type = 'MeerkatTelescopeTapeProduct'

    def _get_value_from_katsensor(self):
        """Populate self.metadata with information from a katcp request"""
        sensor_name_to_get = 'buffer_dir'
        client = BlockingClient(self._sensor_host, self._katcp_port)
        client.start()
        client.wait_protocol()
        reply, informs = client.blocking_request(Message.request("sensor-value", sensor_name_to_get))
        client.stop()
        client.join()
        return informs[0].arguments[-1]

    def _extract_metadata_from_katsensor(self):
        metadata_key_to_map = 'TapeBufferDirectory'
        katsensor_value  = self._get_value_from_katsensor()
        metadata_key_value = os.path.relpath(katsensor_value, self._repository_path)
        self.metadata[metadata_key_to_map] = metadata_key_value

    def extract_metadata(self):
        if not self._metadata_extracted:
            self._extract_metadata_product_type()
            self._extract_metadata_from_katdata()
            self._extract_metadata_from_katsensor()
            self._metadata_extracted = True
        else:
            print "Metadata already extracted. Set the metadata_extracted attribute to False and run again."

#class KATContPipeExtractor(MetExtractor):
#    """Used for extracting metdata from a KAT Cont Pipe VOTable xml file.
#
#    Parameters
#    ----------
#    votable : ElementTree
#        An xml string
#
#    Attributes
#    ----------
#    """
#    def __init__(self, project):
#        super(KAT7MetExtractor, self).__init__()
#        self.votable = votable
#        self.metadata['ProductType'] = 'KATContPipeReductionProduct'
#
#    def extract_metadata(self):
#        """Populate self.metadata with information scraped from a votable xml file"""
#        self.metadata['aipsVer'] = project['aipsVer']
#        self.metadata['AmpCals'] = project['AmpCals'].split()
#        self.metadata['anNames'] = project['anNames'].split()
#        self.metadata['archFileID'] = project['archFileID']
#        self.metadata['BPCals'] = project['BPCals'].split()
#        self.metadata['dataSet'] = project['dataSet']
#        self.metadata['DlyCals'] = project['DlyCals'].split()
#        self.metadata['fileSetID'] = project['fileSetID']
#        self.metadata['freqCov'] = project[float(f) for f in project['freqCov'].split()]
#        self.metadata['minFringe'] = float(project['minFringe'])
#        self.metadata['obitVer'] = project['obitVer']
#        self.metadata['PhsCals'] = project['PhsCals'].split()
#        self.metadata['pipeVer'] = project['pipeVer']
#        self.metadata['procDate'] = project['procDate']
#        self.metadata['project'] = project['project']
#        self.metadata['pyVer'] = project['pyVer']
#        self.metadata['session'] = project['sessioin']
#        self.metadata['sysInfo'] = project['sysInfo']
#
