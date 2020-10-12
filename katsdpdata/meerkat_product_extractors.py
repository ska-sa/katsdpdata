import os
import katdal
import katsdptelstate
from .met_extractors import MetExtractor, TelescopeProductMetExtractor


class MeerKATTelescopeProductMetExtractor(TelescopeProductMetExtractor):
    """A class for handling MeerKAT telescope metadata extraction from a katdal object.

    Parameters
    ----------
    cbid_stream_rdb_file : string : The full path name of the capture stream
    rdb file.
    """
    def __init__(self, cbid_stream_rdb_file):
        katdata = katdal.open(cbid_stream_rdb_file)
        metfilename = '{}.met'.format(katdata.source.data.name)
        super(MeerKATTelescopeProductMetExtractor, self).__init__(katdata, metfilename)
        self.product_type = 'MeerKATTelescopeProduct'

    def extract_metadata(self):
        """Metadata to extract for this product. Test value of self.__metadata_extracted. If
        True, this method has already been run once. If False, extract metadata.
        This includes:
            * extracting the product type
            * extracting basic hdf5 information
            * extacting project related information
        """
        if not self._metadata_extracted:
            self._extract_metadata_product_type()
            self._extract_metadata_from_katdata()
            self._extract_metadata_for_project()
            self._extract_metadata_for_capture_stream()
            self._extract_location_from_katdata()
            self._extract_instrument_name()
            self._metadata_extracted = True
        else:
            print("Metadata already extracted. Set the metadata_extracted attribute to False and run again.")

    def _extract_metadata_for_capture_stream(self):
        """Extract CaptureStreamId, CaptureBlockId and StreamId.
        """
        self.metadata['CaptureBlockId'] = self._katdata.source.metadata.attrs['capture_block_id']
        self.metadata['StreamId'] = self._katdata.source.metadata.attrs['stream_name']
        self.metadata['CaptureStreamId'] = self.metadata['CaptureBlockId'] + '_' + self.metadata['StreamId']
        self.metadata['Prefix'] = self._katdata.source.data.vis_prefix

    def _extract_metadata_product_type(self):
        """Override base method. Extract product type to CAS.ProductTypeName.
        """
        self.metadata['CAS.ProductTypeName'] = self.product_type

    def _extract_instrument_name(self):
        """Exrac the instrument from the enviroment variable if it exists"""
        if 'INSTRUMENT' in os.environ:
            self.metadata['Instrument'] = os.environ['INSTRUMENT']


class MeerKATFlagProductMetExtractor(MetExtractor):
    """A class for handling MeerKAT flag metadata extraction from a rdb file.

    Parameters
    ----------
    cbid_stream_rdb_file : string : The full path name of the capture stream
    rdb file.
    """
    def __init__(self, cbid_stream_rdb_file):
        self._ts = katsdptelstate.TelescopeState()
        self._ts.load_from_file(cbid_stream_rdb_file)
        metfilename = '{}.met'.format(self._ts['capture_block_id']+'_'+self._ts['stream_name'])
        super(MeerKATFlagProductMetExtractor, self).__init__(metfilename)
        self.product_type = 'MeerKATFlagProduct'

    def extract_metadata(self):
        """Metadata to extract for this product. Test value of self.__metadata_extracted. If
        True, this method has already been run once. If False, extract metadata.
        This includes:
            * extracting the product type
        """
        if not self._metadata_extracted:
            self._extract_metadata_product_type()
            self._extract_metadata_for_capture_stream()
            self._extract_instrument_name()
            self._metadata_extracted = True
        else:
            print("Metadata already extracted. Set the metadata_extracted attribute to False and run again.")

    def _extract_metadata_for_capture_stream(self):
        """Extract CaptureStreamId, CaptureBlockId and StreamId.
        """
        self.metadata['CaptureBlockId'] = self._ts['capture_block_id']
        self.metadata['StreamId'] = self._ts['stream_name']
        self.metadata['CaptureStreamId'] = self.metadata['CaptureBlockId'] + '_' + self.metadata['StreamId']

    def _extract_metadata_product_type(self):
        """Override base method. Extract product type to CAS.ProductTypeName.
        """
        self.metadata['CAS.ProductTypeName'] = self.product_type

    def _extract_instrument_name(self):
        """Extract the instrument from the enviroment variable if it exists.
        """
        if 'INSTRUMENT' in os.environ:
            self.metadata['Instrument'] = os.environ['INSTRUMENT']
