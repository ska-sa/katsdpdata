import os
import subprocess
import sys
import time
import pickle
import katpoint
import katdal
import time
import datetime
import numpy
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
        self.metadata['CAS.ProductTypeName'] = self.product_type

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
        #utf8 hickup
        reload(sys)
        sys.setdefaultencoding('utf8')
        return ElementTree.tostring(xml_tree)

    def extract_metadata(self):
        raise NotImplementedError

    def write_metadatafile(self):
        if self._metadata_extracted:
            with open(self.metadata_filename, 'w') as metfile:
                metfile.write(str(self))
        else:
            raise MetExtractorException('No metadata extracted.')

class TelescopeProductMetExtractor(MetExtractor):
    """A class for handling telescope systems metadata extraction. This class contains
    a factory method which returns the correct met extractor object to use. The following 
    systems are currently supported: KAT7, RTS, MeerKAT AR1.

    Use the static 'factory' method from this class.

    Parameters
    ----------
    katdata : object : katdal object 
        A valid katdal oject.
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
        self.metadata['Bandwidth'] = str(max(self._katdata.freqs) - min(self._katdata.freqs) + self._katdata.channel_width)
        self.metadata['Description'] = self._katdata.description
        self.metadata['Details'] = str(self._katdata)
        self.metadata['DumpPeriod'] = '%.4f' % (self._katdata.dump_period)
        self.metadata['Duration'] = str(round(self._katdata.end_time-self._katdata.start_time, 2))
        self.metadata['ExperimentID'] = self._katdata.experiment_id
        self.metadata['FileSize'] = str(os.path.getsize(self._katdata.file.filename))
        self.metadata['KatfileVersion'] = self._katdata.version
        self.metadata['KatpointTargets'] = [t.description for t in self._katdata.catalogue.targets if t.name not in ['None', 'Nothing']]
        self.metadata['NumFreqChannels'] = str(len(self._katdata.channels))
        self.metadata['Observer'] = self._katdata.observer
        self.metadata['RefAntenna'] = self._katdata.ref_ant
        self.metadata['StartTime'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(self._katdata.start_time))
        self.metadata['Targets'] = [t.name for t in self._katdata.catalogue.targets if t.name not in ['None', 'Nothing', 'azel', 'radec']]

        try:
            self.metadata['InstructionSet'] = '%s %s' % (self._katdata.obs_params['script_name'], self._katdata.obs_params['script_arguments'])
        except KeyError:
            pass

    def _extract_location_from_katdata(self):

        self.metadata["DecRa_path"]=[]
        self.metadata["DecRa"]=[]
        self.metadata["ElAz_path"]=[]
        self.metadata["ElAz"]=[]

        f = self._katdata
        f.select(scans="track,scan")
        f.select(ants=f.ref_ant)

        for i, scan, target in f.scans():
            f.select(scans=i)
            t = f.catalogue.targets[f.target_indices[0]]
            if (target.body_type != 'radec'):
                self.metadata["DecRa_path"] += ["%f,%f"%(dec,katpoint.wrap_angle(ra,360)) for ra,dec in zip(f.ra[:,0],f.dec[:,0])]

            else:
                self.metadata["DecRa"].append("%f,%f"%(numpy.mean(f.dec),numpy.mean(katpoint.wrap_angle(f.ra,360))))

            if (target.body_type != 'azel'):
                if max(f.el) <= 90 and min(f.el) >= -90:
                    self.metadata["ElAz_path"] += ["%f,%f"%(el,katpoint.wrap_angle(az,360)) for az,el in zip(f.az[:,0],f.el[:,0])]
                else:
                    self.metadata["ElAz_path"] += ["%f,%f"%(numpy.clip(el,-90,90),katpoint.wrap_angle(az,360)) for az,el in zip(f.az[:,0],f.el[:,0])]

            else:
                if -90 <= numpy.mean(f.el) <= 90:
                    self.metadata["ElAz"].append("%f,%f"%(numpy.mean(f.el),numpy.mean(katpoint.wrap_angle(f.az,360))))
                else:
                    self.metadata["ElAz"].append("%f,%f"%(numpy.mean(numpy.clip(f.el,-90,90)),numpy.mean(katpoint.wrap_angle(f.az,360))))

            if len(self.metadata["DecRa_path"]) > 2000:
                self.metadata["DecRa_path"] = self.metadata["DecRa_path"][::len(self.metadata["DecRa_path"])/2000 + 1] + [self.metadata["DecRa_path"][-1],]
            if len(self.metadata["ElAz_path"]) > 2000:
                self.metadata["ElAz_path"] = self.metadata["ElAz_path"][::len(self.metadata["ElAz_path"])/2000 + 1] + [self.metadata["ElAz_path"][-1],]

    def _extract_metadata_for_project(self):
        """Populate self.metadata: Grab if available proposal, program block and project id's from the observation script arguments."""
        #ProposalId
        if 'proposal_id' in self._katdata.obs_params:
            self.metadata['ProposalId'] = self._katdata.obs_params['proposal_id']
        #ProgramBlockId
        if 'program_block_id' in self._katdata.obs_params:
            self.metadata['ProgramBlockId'] = self._katdata.obs_params['program_block_id']
        #ScheduleBlockId
        if 'sb_id_code' in self._katdata.obs_params:
            self.metadata['ScheduleBlockIdCode'] = self._katdata.obs_params['sb_id_code']
        #IssueId
        if 'issue_id' in self._katdata.obs_params and self._katdata.obs_params['issue_id'] != '':
            self.metadata['IssueId'] = self._katdata.obs_params['issue_id']
        #ProposalDescription
        if 'proposal_description' in self._katdata.obs_params and self._katdata.obs_params['proposal_description'] != '':
            self.metadata['ProposalDescription'] = self._katdata.obs_params['proposal_description']

    def _extract_metadata_file_digest(self):
        """Populate self.metadata: Calculate the md5 checksum and create a digest metadata key"""
        md5_filename = os.path.abspath(self.katfile + '.md5')
        if os.path.isfile(md5_filename):
            with open(md5_filename, 'r') as md5:
                 self.metadata['FileDigest']= md5.read().strip()
                 print 'Digest is %s.' % self.metadata['FileDigest']
            os.remove(md5_filename)
        else:
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
        file_ext = katfile[-2:]
        if file_ext == 'sf': 
            #pulsar search file
            return PulsarSearchProductMetExtractor(katfile)
        elif file_ext == 'ar':
            #pulsar timing archive file
            return PulsarTimingArchiveProductMetExtractor(katfile)
        elif file_ext == 'h5': #Correlator data  Remove and put in crawler
            katdata = katdal.open(katfile)
            #atleast one antenna starts with 'ant'
            if katdata.ants[0].name.startswith('ant'):
                #todo: replace with KAT7TelescopeProductMetExtractor
                return KatFileProductMetExtractor(katdata)
            #proposal id must mention RTS at least once
            elif 'proposal_id' in katdata.obs_params and katdata.obs_params['proposal_id'].count('RTS') >= 1:
                return RTSTelescopeProductMetExtractor(katdata)
            #everything else must be ar1
            else:
                return MeerKATAR1TelescopeProductMetExtractor(katdata)

class KAT7TelescopeProductMetExtractor(TelescopeProductMetExtractor):
    """Used for extracting metadata for a KAT7 Telescope product. As well as extracting data from a
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
            self._extract_location_from_katdata()
            self._metadata_extracted = True
        else:
           print "Metadata already extracted. Set the metadata_extracted attribute to False and run again." 

class KatFileProductMetExtractor(KAT7TelescopeProductMetExtractor):
    def __init__(self, katdata):
        super(KatFileProductMetExtractor, self).__init__(katdata)
        self.product_type = 'KatFile'

class RTSTelescopeProductMetExtractor(TelescopeProductMetExtractor):
    """Used for extracting metadata for a RTSTelescopeProduct. As well as extracting data from a
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
            * extracting project info
            * extracting an md5 checksum
        """
        if not self._metadata_extracted:
            self._extract_metadata_product_type()
            self._extract_metadata_from_katdata()
            self._extract_metadata_for_auto_reduction()
            self._extract_metadata_file_digest()
            self._extract_metadata_for_project()
            self._extract_location_from_katdata()
            self._metadata_extracted = True
        else:
            print "Metadata already extracted. Set the metadata_extracted attribute to False and run again."

class MeerKATAR1TelescopeProductMetExtractor(TelescopeProductMetExtractor):
    """Used for extracting metadata for a MeerKATAR1TelescopeProduct.

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

    def _extract_sub_array_details(self):
        try:
            self.metadata['SubarrayProductId'] = pickle.loads(self._katdata.file['TelescopeState'].attrs['subarray_product_id'])
            self.metadata['SubarrayNumber'] = pickle.loads(self._katdata.file['TelescopeState'].attrs['sub_sub_nr'])
            self.metadata['SubarrayProduct'] = pickle.loads(self._katdata.file['TelescopeState'].attrs['sub_product'])

        except IndexError:
            self.metadata['SubarrayProductId'] = self._katdata.file['TelescopeState'].attrs['subarray_product_id']
            self.metadata['SubarrayNumber'] = self._katdata.file['TelescopeState'].attrs['sub_sub_nr']
            self.metadata['SubarrayProduct'] = self._katdata.file['TelescopeState'].attrs['sub_product']

    def _extract_metadata_for_auto_reduction(self):
        """Populate self.metadata with information scraped from self"""
        metadata_key_to_map = 'ReductionLabel'
        obs_param_to_get = 'reduction_label'
        obs_param = self._katdata.obs_params.get(obs_param_to_get)
        if not obs_param:
            obs_param = self._katdata.obs_params.get('reduction_name')
        if obs_param:
            self.metadata[metadata_key_to_map] = obs_param

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
            self._extract_metadata_file_digest()
            self._extract_metadata_product_type()
            self._extract_metadata_from_katdata()
            self._extract_metadata_for_project()
            self._extract_sub_array_details()
            self._extract_metadata_for_auto_reduction()
            self._extract_location_from_katdata()
            self._metadata_extracted = True
        else:
            print "Metadata already extracted. Set the metadata_extracted attribute to False and run again."

class MeerkatTelescopeTapeProductMetExtractor(TelescopeProductMetExtractor):
    """Used for extracting metadata for a MeerkatTelescopeTapeProduct. As well as extracting data
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

class ReductionProductMetExtractor(MetExtractor):
    """A base class for handling reduction systems metadata extraction.

    self.product_type is not set

    Parameters
    ----------
    prod_name : string : the name of a heirachical product to ingest.
    """
    def __init__(self, prod_name):
        self.picklefile = None
        picklefile = next((p for p in os.listdir(prod_name) if p.endswith('.met.pickle')), None)
        if picklefile:
            self._picklefile = os.path.join(prod_name, picklefile)
        else:
            raise MetExtractorException('Cannot find a *.met.pickle file in %s' % (prod_name))
        super(ReductionProductMetExtractor, self).__init__('%s.%s' % (prod_name, 'met',))

    def extract_metadata(self):
        if not self._metadata_extracted:
            self._extract_metadata_product_type()
            self._extract_metadata_from_pickle()
            self._metadata_extracted = True
        else:
            print "Metadata already extracted. Set the metadata_extracted attribute to False and run again."

    def _extract_metadata_from_pickle(self):
        with open(self._picklefile) as pickled_met:
            self.metadata.update(pickle.load(pickled_met))

class RTSReductionProductMetExtractor(ReductionProductMetExtractor):
    """A class for handling RTS reduction systems metadata extraction.

    Parameters
    ----------
    prod_name : string : the name of a heirachical product to ingest.
    """
    def __init__(self, prod_name):
        super(RTSReductionProductMetExtractor, self).__init__(prod_name)
        self.product_type = 'RTSReductionProduct'

class MeerKATAR1ReductionProductMetExtractor(ReductionProductMetExtractor):
    """A class for handling AR1 reduction systems metadata extraction.

    Parameters
    ----------
    prod_name : string : the name of a heirachical product to ingest.
    """
    def __init__(self, prod_name):
        super(MeerKATAR1ReductionProductMetExtractor, self).__init__(prod_name)
        self.product_type = 'MeerKATAR1ReductionProduct'

class ObitReductionProductMetExtractor(MetExtractor):
    """Used for extracting metdata from a KAT Cont Pipe VOTable xml file.

    Parameters
    ----------
    prod_name : string : the name of a heirachical product to ingest.
    def __init__(self, prod_name):
            super(PulsarSearchProductMetExtractor, self).__init__(prod_name+'.met')
                    self.product_type = 'PulsarSearchProduct'
                            self.product_name = prod_name"""
    def __init__(self, prod_name):
        votable_file = next((p for p in os.listdir(prod_name) if p.endswith('_VOTable.xml')), None)
        tree = ElementTree.parse(os.path.join(prod_name, votable_file))
        votable = tree.getroot()
        for child in votable:
            if child.tag == 'resource' and child.attrib['name'] == 'Project Data':
                project_data = child
                break
        if project_data:
            self.project_data = project_data
        else:
            raise MetExtractorException('Cannot find a *_VOTable.xml file in %s' % (prod_name))
        super(ObitReductionProductMetExtractor, self).__init__('%s.%s' % (prod_name, 'met',))
        self.product_type = 'ObitReductionProduct'

    def extract_metadata(self):
        if not self._metadata_extracted:
            self._extract_metadata_product_type()
            self._extract_metadata_from_votable()
            self._metadata_extracted = True
        else:
            print "Metadata already extracted. Set the metadata_extracted attribute to False and run again."

    def _extract_metadata_from_votable(self):
        met = dict([[param.attrib['name'],param.attrib['value']] for param in self.project_data.getchildren() if param.tag == 'param'])
        mult_valued = ['AmpCals', 'BPCals', 'DlyCals', 'PhyCals', 'PhsCals', 'anNames', 'freqCov']
        date_valued = ['obsDate', 'procDate']
        for k,v in met.iteritems():
            if k in mult_valued:
                met[k] = ' '.join(v.split()).split()
            if k in date_valued:
                met[k] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.strptime(v, '%Y-%m-%d'))
        self.metadata.update(met)

class BeaformerProductMetExtractor(MetExtractor):

    def __init__(self, prod_name):
        super(BeaformerProductMetExtractor, self).__init__(prod_name+'.met')
        self.product_type = 'PulsarSearchProduct'
        self.product_name = prod_name

    def _extract_locations(self):
        print self.metadata
        if 'KatpointTargets' in self.metadata and "StartTime" in self.metadata and 'Duration' in self.metadata:
            self.metadata["DecRa_path"]=[]
            self.metadata["DecRa"]=[]
            self.metadata["ElAz_path"]=[]
            self.metadata["ElAz"]=[]
            
            for t in self.metadata['KatpointTargets']:
                target = katpoint.Target(t)
                if (target.body_type != 'radec'):
                    try:
                        start = time.mktime(datetime.datetime.strptime(self.metadata["StartTime"], '%Y-%m-%dT%H:%M:%SZ').timetuple())
                        for ts in range(int(start), int(start + float(self.metadata['Duration'])), 10) + [start + float(self.metadata['Duration']),]:
                            self.metadata["DecRa_path"].append("%f,%f"%(target.radec(ts)[1],target.radec(ts)[0]))
                    except ValueError as e:
                        print e

                else:
                    self.metadata["DecRa"].append("%f,%f"%(target.radec()[1],target.radec()[0]))

                if (target.body_type == 'azel'):
                    self.metadata["ElAz"].append("%f,%f"%(target.azel(ts)[1],target.azel(ts)[0]))

                if len(self.metadata["DecRa_path"]) > 2000:
                    self.metadata["DecRa_path"] = self.metadata["DecRa_path"][::len(self.metadata["DecRa_path"])/2000 + 1] + [self.metadata["DecRa_path"][-1],]
                if len(self.metadata["ElAz_path"]) > 2000:
                    self.metadata["ElAz_path"] = self.metadata["ElAz_path"][::len(self.metadata["ElAz_path"])/2000 + 1] + [self.metadata["ElAz_path"][-1],]
        else:
            print "Does not have required metadata"
            raise NotEnoughMetadata

class NotEnoughMetadata(Exception):
    """Raised if there is not enough metadata to calculate paths"""
    pass

class PulsarSearchProductMetExtractor(BeaformerProductMetExtractor):
    """Used for extracting metdata from a KAT Cont Pipe VOTable xml file.

    Parameters
    ----------
    prod_name : string : the name of a heirachical product to ingest.
    """
    def __init__(self, prod_name):
        super(PulsarSearchProductMetExtractor, self).__init__(prod_name)
        self.product_type = 'PulsarSearchProduct'
        self.product_name = prod_name
    
    def extract_metadata(self):
        print "extracting product type"
        self._extract_metadata_product_type()
        print "extracting fits"
        self.extract_fits_header()
        print "extracting location"
        self._extract_locations()
   
    def extract_fits_header(self):
        print "fits header"
        import pyfits
        data_files = os.listdir(self.product_name)
        count = 0
    
        print data_files
        while data_files[count][-2:] != 'sf':
            print data_files[count][-2:]
            count+=1
        data = pyfits.open("%s/%s"%(self.product_name,data_files[count]), memmap=True, ignore_missing_end=True)
        obs_info_file = open ("%s/obs_info.dat"%self.product_name)
        obs_info = dict([a.split(';') for a in obs_info_file.read().split('\n')[:-1]])
        self.metadata["Observer"]=obs_info["observer"]
        self.metadata["ProgramBlockId"]=obs_info["program_block_id"]
        self.metadata["Targets"]=obs_info["targets"]
        self.metadata["ScheduleBlockIdCode"]=obs_info["sb_id_code"]
        self.metadata["Duration"]=obs_info["target_duration"]
        self.metadata["ProposalId"]=obs_info["proposal_id"]
        self.metadata["Description"]=obs_info["description"]
        self.metadata["ExperimentID"]=obs_info["experiment_id"]
        self.metadata["CAS.ProductTypeName"]='PulsarSearchProduct'
    
        hduPrimary = data[0].header
        hduSubint = data[2].header
        self.metadata["STT_CRD1"]=str(hduPrimary["STT_CRD1"])
        self.metadata["STT_CRD2"]=str(hduPrimary["STT_CRD2"])
        self.metadata["STP_CRD1"]=str(hduPrimary["STP_CRD1"])
        self.metadata["STP_CRD2"]=str(hduPrimary["STP_CRD2"])
        self.metadata["TRK_MODE"]=str(hduPrimary["TRK_MODE"])
        self.metadata["CAL_MODE"]=str(hduPrimary["CAL_MODE"])
        self.metadata["Bandwidth"]=str(hduPrimary["OBSBW"]*1000000)
        self.metadata["NPOL"]=str(hduSubint["NPOL"])
        self.metadata["POL_TYPE"]=str(hduSubint["POL_TYPE"])
        self.metadata["ScheduleBlockIdCode"]=obs_info["sb_id_code"]
        self.metadata['Description'] = obs_info["description"]
        self.metadata['ExperimentID'] = obs_info["experiment_id"]
        self.metadata['FileSize'] = str(sum(os.path.getsize(f) for f in os.listdir(self.product_name) if os.path.isfile(f)))
        self.metadata['KatfileVersion'] = "sf"
        self.metadata['Observer'] = obs_info["observer"]
        self.metadata['StartTime'] = "%sZ"%hduPrimary["DATE"]
        import ast
        self.metadata['Targets'] = ast.literal_eval(obs_info["targets"])
   
        import subprocess
        import katpoint
        cmd = ["psrcat","-c","'RAJ DECJ'", "-o", "short_csv", " ".join([t for t in self.metadata['Targets'] if not t.startswith('azel')])]
        psrstat_process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        output, err = psrstat_process.communicate()
        self.metadata['KatpointTargets'] = []

        for line in zip([t for t in self.metadata['Targets'] if not t.startswith('azel') and not t.startswith('Azel')],output.split('\n')[2:]):
            rds = line[1].split(';')
            t = katpoint.Target("%s, radec,%s,%s"%(line[0],rds[1],rds[2]))
            self.metadata['KatpointTargets'].append(t.description)
            self.metadata['Azel'] = []

        for t in [t for t in self.metadata['Targets'] if t.startswith('azel') or t.startswith('Azel')]:
            try:
                target = katpoint.Target(t)
                self.metadata['KatpointTargets'].append(target.description)
            except ValueError:
                print e
                pass
                    
        print "fits extracted"
        print self.metadata
        self._metadata_extracted = True

#input string of ra and dec in hours and return floats with the degree values
def hoursToDegrees(ra,dec):
    ralist = [float(v) for v in ra.split(':')]
    declist = [float(v) for v in dec.split(':')]
    
    raDeg = ralist[0]*15 + ralist[1]*15/60 + ralist[2]*15/3600
    decDeg = declist[0] + declist[1]/60 + declist[2]/3600

    return raDeg,decDeg
         
class PulsarTimingArchiveProductMetExtractor(BeaformerProductMetExtractor):
    """Used for extracting metdata from a KAT Cont Pipe VOTable xml file.

    Parameters
    ----------
    prod_name : string : the name of a heirachical product to ingest.
    """

    def __init__(self, prod_name):
        super(PulsarTimingArchiveProductMetExtractor, self).__init__(prod_name)
        self.product_type = 'PulsarTimingArchiveProduct'        
        self.product_name = prod_name
 
    def extract_metadata(self):
        self._extract_metadata_product_type()
        self.extract_archive_header()
        self._extract_locations()

    def extract_archive_header(self):
        from datetime import datetime
        data_files = os.listdir(self.product_name)
        data_files = os.listdir(self.product_name)
        sort = sorted(data_files)
        obs_info_file = open ("%s/obs_info.dat"%self.product_name)
        obs_info = dict([a.split(';') for a in obs_info_file.read().split('\n')[:-1]])

        self.metadata["Observer"]=obs_info["observer"]
        self.metadata["ProgramBlockId"]=obs_info["program_block_id"]
        self.metadata["Targets"]=obs_info["targets"]
        self.metadata["ScheduleBlockIdCode"]=obs_info["sb_id_code"]
        self.metadata["Duration"]=obs_info["target_duration"]
        self.metadata["ProposalId"]=obs_info["proposal_id"]
        self.metadata["Description"]=obs_info["description"]
        self.metadata["ExperimentID"]=obs_info["experiment_id"]
        self.metadata["CAS.ProductTypeName"]='PulsarTimingArchiveProduct'
        self.metadata["ScheduleBlockIdCode"]=obs_info["sb_id_code"]
        self.metadata['Description'] = obs_info["description"]
        self.metadata['FileSize'] = str(sum(os.path.getsize(f) for f in os.listdir(self.product_name) if os.path.isfile(f)))
        self.metadata['KatfileVersion'] = "ar"
        import ast
        self.metadata['Targets'] = ast.literal_eval(obs_info["targets"])
        self.metadata['StartTime'] = "%sT%sZ"%(obs_info["UTC_START"][:10],obs_info["UTC_START"][11:])

        import subprocess
        import katpoint
        cmd = ["psrcat","-c","RAJ DECJ", "-o", "short_csv", " ".join(self.metadata['Targets'])]
        psrstat_process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        output, err = psrstat_process.communicate()

        self.metadata['KatpointTargets'] = []
        for line in zip(self.metadata['Targets'],output.split('\n')[2:]):
            rds = line[1].split(';')
            t = katpoint.Target("%s, radec,%s,%s"%(line[0],rds[1],rds[2]))
            self.metadata['KatpointTargets'].append(t.description)

        cmd = ["psrstat","-Q","%s/%s"%(self.product_name,sort[0]),"-c","bw"]
        psrstat_process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        output, err = psrstat_process.communicate()
        bandwidth = float(output.split(' ')[1]) * 1000000
        self.metadata['Bandwidth'] = str(bandwidth)

        self._metadata_extracted = True

class PTUSETimingArchiveProductMetExtractor(BeaformerProductMetExtractor):
    """Used for extracting metdata from a KAT Cont Pipe VOTable xml file.

    Parameters
    ----------
    prod_name : string : the name of a heirachical product to ingest.
    """
    def __init__(self, prod_name):
        super(PTUSETimingArchiveProductMetExtractor, self).__init__(prod_name)
        self.product_type = 'PTUSETimingArchiveProduct'
        self.product_name = prod_name

    def extract_metadata(self):
        self._extract_metadata_product_type()
        self._extract_archive_header()
        self._extract_locations()

    def _extract_archive_header(self):
        data_files = os.listdir(self.product_name)
        sort = sorted(data_files)
        import subprocess
        from astropy.time import Time
        cmd = ["psrstat","-Q","%s/%s"%(self.product_name,sort[0]),"-c","ext:stt_smjd,ext:stt_imjd,ext:ra,ext:dec,bw"]
        psrstat_process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        output, err = psrstat_process.communicate()
        radec = hoursToDegrees(output.split(' ')[3], output.split(' ')[4])
        imjd = output.split(' ')[2]
        smjd = output.split(' ')[1]
        start_time = Time([float(imjd) + float(smjd) / 3600.0 / 24.0],format='mjd')
        start_time.format = 'isot'
        startTime =start_time.value[0][:-4]+'Z'
        bandwidth=float(output.split(' ')[5]) * 1000000
        
        obs_info_file = open ("%s/obs_info.dat"%self.product_name)
        obs_info = dict([a.split(';') for a in obs_info_file.read().split('\n')[:-1]])
        self.metadata["Observer"]=obs_info["observer"]
        self.metadata["Bandwidth"]=str(bandwidth)
        self.metadata["ProgramBlockId"]=obs_info["program_block_id"]
        self.metadata["ScheduleBlockIdCode"]=obs_info["sb_id_code"]
        self.metadata["Duration"]=obs_info["target_duration"]
        self.metadata["ProposalId"]=obs_info["proposal_id"]
        self.metadata["Description"]=obs_info["description"]
        self.metadata["ExperimentID"]=obs_info["experiment_id"]
        self.metadata["CAS.ProductTypeName"]='PTUSETimingArchiveProduct'
        self.metadata["ScheduleBlockIdCode"]=obs_info["sb_id_code"]
        self.metadata['Description'] = obs_info["description"]
        self.metadata['FileSize'] = str(sum(os.path.getsize(f) for f in os.listdir(self.product_name) if os.path.isfile(f)))
        self.metadata['KatfileVersion'] = "ar"
        import ast
        self.metadata['Targets'] = ast.literal_eval(obs_info["targets"])
        self.metadata['StartTime'] = startTime

        import katpoint
        t = katpoint.Target("%s, radec, %s, %s"%(self.metadata["Targets"][0],radec[0],radec[1]))
        self.metadata['KatpointTargets'] = [t.description]
        self._metadata_extracted = True
