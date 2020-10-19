import os
import subprocess
import sys
import time
import pickle
import katpoint
import katdal
import numpy

from importlib import reload
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
        # utf8 hickup
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
    """A parent class to specifically handel file based MeerKAT telescope metadata extraction from a katdal object.

    Parameters
    ----------
    katdata : object : katdal object
        A valid katdal oject.
    """
    def __init__(self, katdata, metfilename):
        self._katdata = katdata
        super(TelescopeProductMetExtractor, self).__init__(metfilename)

    def _extract_metadata_from_katdata(self):
        """Populate self.metadata: Get information using katdal"""
        self.metadata['Antennas'] = [a.name for a in self._katdata.ants]
        self.metadata['CenterFrequency'] = str(self._katdata.channel_freqs[self._katdata.channels[-1]/2])
        self.metadata['ChannelWidth'] = str(self._katdata.channel_width)
        self.metadata['MinFreq'] = str(min(self._katdata.freqs))
        self.metadata['MaxFreq'] = str(max(self._katdata.freqs) + self._katdata.channel_width)
        self.metadata['Bandwidth'] = str(max(self._katdata.freqs) - min(self._katdata.freqs) + self._katdata.channel_width)
        self.metadata['Description'] = self._katdata.description
        self.metadata['Details'] = str(self._katdata)
        self.metadata['DumpPeriod'] = '%.4f' % (self._katdata.dump_period)
        self.metadata['Duration'] = str(round(self._katdata.end_time-self._katdata.start_time, 2))
        self.metadata['ExperimentID'] = self._katdata.experiment_id
        if self._katdata.file:
            self.metadata['FileSize'] = str(os.path.getsize(self._katdata.file.filename))
        else:
            self.metadata['FileSize'] = str(self._katdata.size)
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
        self.metadata["DecRa"] = []
        self.metadata["ElAz"] = []

        f = self._katdata
        f.select(scans="track,scan")
        f.select(ants=f.ref_ant)

        for i, scan, target in f.scans():
            f.select(scans=i)
            t = f.catalogue.targets[f.target_indices[0]]
            if (t.body_type == 'radec'):
                ra, dec = t.radec()
                ra, dec = katpoint.rad2deg(ra), katpoint.rad2deg(dec)
                self.metadata["DecRa"].append("%f, %f" % (dec, katpoint.wrap_angle(ra, 360)))

            elif t.body_type == 'azel':
                az, el = t.azel()
                az, el = katpoint.rad2deg(az), katpoint.rad2deg(el)
                if -90 <= el <= 90:
                    self.metadata["ElAz"].append("%f, %f" % (el, katpoint.wrap_angle(az, 360)))
                else:
                    self.metadata["ElAz"].append("%f, %f" % ((numpy.clip(el, -90, 90)), katpoint.wrap_angle(az, 360)))

    def _extract_metadata_for_project(self):
        """Populate self.metadata: Grab if available proposal, program block and project id's from the observation script arguments."""
        # ProposalId
        if 'proposal_id' in self._katdata.obs_params:
            self.metadata['ProposalId'] = self._katdata.obs_params['proposal_id']
        # ProgramBlockId
        if 'program_block_id' in self._katdata.obs_params:
            self.metadata['ProgramBlockId'] = self._katdata.obs_params['program_block_id']
        # ScheduleBlockId
        if 'sb_id_code' in self._katdata.obs_params:
            self.metadata['ScheduleBlockIdCode'] = self._katdata.obs_params['sb_id_code']
        # IssueId
        if 'issue_id' in self._katdata.obs_params and self._katdata.obs_params['issue_id'] != '':
            self.metadata['IssueId'] = self._katdata.obs_params['issue_id']
        # ProposalDescription
        if 'proposal_description' in self._katdata.obs_params and self._katdata.obs_params['proposal_description'] != '':
            self.metadata['ProposalDescription'] = self._katdata.obs_params['proposal_description']


class FileBasedTelescopeProductMetExtractor(TelescopeProductMetExtractor):
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
        self.katfile = os.path.abspath(katdata.file.filename)
        super(FileBasedTelescopeProductMetExtractor, self).__init__(katdata, '%s.%s' % (self.katfile, 'met',))

    def _extract_metadata_file_digest(self):
        """Populate self.metadata: Calculate the md5 checksum and create a digest metadata key"""
        md5_filename = os.path.abspath(self.katfile + '.md5')
        if os.path.isfile(md5_filename):
            with open(md5_filename, 'r') as md5:
                self.metadata['FileDigest'] = md5.read().strip()
                print('Digest is %s.' % self.metadata['FileDigest'])
            os.remove(md5_filename)
        else:
            print('Calculating the md5 checksum for %s. This may take a while.' % (self.katfile))
            p = subprocess.Popen(['md5sum', self.katfile], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            if not p[1]:
                self.metadata['FileDigest'] = p[0].split()[0]
                print('md5 checksum complete. Digest is %s.' % self.metadata['FileDigest'])

    @staticmethod
    def factory(katfile):
        """Static method to instantiate the correct metadata extraction object. The following systems
        are currently supported: KAT7, RTS, MeerKAT AR1.

        Parameters:
        ----------
        katfile: string : name of file to opened with the katdal module.
        """
        file_ext = os.path.splitext(katfile)[1]
        if file_ext == '.h5':  # Correlator data  Remove and put in crawler
            katdata = katdal.open(katfile)
            # atleast one antenna starts with 'ant'
            if katdata.ants[0].name.startswith('ant'):
                # todo: replace with KAT7TelescopeProductMetExtractor
                return KatFileProductMetExtractor(katdata)
            # proposal id must mention RTS at least once
            elif 'proposal_id' in katdata.obs_params and katdata.obs_params['proposal_id'].count('RTS') >= 1:
                return RTSTelescopeProductMetExtractor(katdata)
            # everything else must be ar1
            else:
                return MeerKATAR1TelescopeProductMetExtractor(katdata)


class KAT7TelescopeProductMetExtractor(FileBasedTelescopeProductMetExtractor):
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
            print("Metadata already extracted. Set the metadata_extracted attribute to False and run again.")


class KatFileProductMetExtractor(KAT7TelescopeProductMetExtractor):
    def __init__(self, katdata):
        super(KatFileProductMetExtractor, self).__init__(katdata)
        self.product_type = 'KatFile'


class RTSTelescopeProductMetExtractor(FileBasedTelescopeProductMetExtractor):
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
        # always set product_type after call to super
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
            print("Metadata already extracted. Set the metadata_extracted attribute to False and run again.")


class MeerKATAR1TelescopeProductMetExtractor(FileBasedTelescopeProductMetExtractor):
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
        # override product_type
        self.product_type = 'MeerKATAR1TelescopeProduct'

    def _extract_sub_array_details(self):
        try:
            self.metadata['SubarrayProductId'] = pickle.loads(self._katdata.file['TelescopeState'].attrs['subarray_product_id'])
            self.metadata['SubarrayNumber'] = str(pickle.loads(self._katdata.file['TelescopeState'].attrs['sub_sub_nr']))
            self.metadata['SubarrayProduct'] = pickle.loads(self._katdata.file['TelescopeState'].attrs['sub_product'])

        except IndexError:
            self.metadata['SubarrayProductId'] = self._katdata.file['TelescopeState'].attrs['subarray_product_id']
            self.metadata['SubarrayNumber'] = str(self._katdata.file['TelescopeState'].attrs['sub_sub_nr'])
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
            print("Metadata already extracted. Set the metadata_extracted attribute to False and run again.")


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
