import katdal
import katsdptelstate
import io
import urllib

from katsdpdata.met_extractors import MetExtractor, TelescopeProductMetExtractor


class DataSourceNotFound(Exception):
    """File associated with DataSource not found or server not responding."""


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

class MeerKATFlagProductMetExtractor(MetExtractor):
    """A class for handling MeerKAT flag metadata extraction from a rdb file.

    Parameters
    ----------
    cbid_stream_rdb_file : string : The full path name of the capture stream
    rdb file.
    """
    def __init__(self, cbid_stream_rdb_url):
        # assiging self._ts copied verbatim from the katdal repository katdal/datasources.py
        url_parts = urllib.parse.urlparse(cbid_stream_rdb_url, scheme='file')
        if url_parts.scheme == 'file':
            # RDB dump file
            self._ts = katsdptelstate.TelescopeState(katsdptelstate.memory.MemoryBackend())
            try:
                self._ts.load_from_file(url_parts.path)
            except OSError as e:
                raise DataSourceNotFound(str(e))
        elif url_parts.scheme in {'http'}:
            # Treat URL prefix as an S3 object store (with auth info in kwargs)
            store_url = urllib.parse.urljoin(cbid_stream_rdb_url, '..')
            # Strip off parameters, query strings and fragments to get basic URL
            rdb_url = urllib.parse.urlunparse(
                (url_parts.scheme, url_parts.netloc, url_parts.path, '', '', ''))
            self._ts = katsdptelstate.TelescopeState(katsdptelstate.memory.MemoryBackend())
            try:
                rdb_store = katdal.chunkstore_s3.S3ChunkStore.from_url(store_url)
                with rdb_store.request('', 'GET', rdb_url) as response:
                    self._ts.load_from_file(io.BytesIO(response.content))
            except katdal.chunkstore.ChunkStoreError as e:
                raise DataSourceNotFound(str(e))
        else:
            raise DataSourceNotFound("Unknown URL scheme '{}' - telstate expects "
                                     "file, redis, or http(s)".format(url_parts.scheme))
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
