import logging
import re


from katsdpdata.meerkat_product_extractors import MeerKATTelescopeProductMetExtractor, MeerKATFlagProductMetExtractor
from katsdpdata.met_handler import MetaDataHandler
from katsdpdata.met_detectors import stream_type_detection


logger = logging.getLogger(__name__)


STREAM_TYPES= {'MeerKATTelescopeProduct' : '^[0-9]{10}[-_]sdp[-_](l0$|l0[-_]continuum$)',
               'MeerKATFlagProduct' : '^[0-9]{10}[-_]sdp[-_](l1[-_]flags$|l1[-_]flags[-_]continuum$)'}


class ProductError(Exception):
    pass


class ProductBase(object):
    """docstring for ProductBase"""
    def __init__(self, name, **kwargs):
        super(ProductBase, self).__init__()
        self.name = name
        self.metadata_id = None
        self._met_handler = None
        if 'solr_endpoint' in kwargs:
            self.set_metadata_handler(kwargs['solr_endpoint'])

    def set_metadata_handler(self, solr_endpoint):
        self._met_handler = MetaDataHandler(solr_endpoint)
        if self._met_handler.prod_met_exists(self.name):
            met = self._met_handler.get_prod_met(self.name)
        else:
            met = self._met_handler.create_core_met(self._product_type(), self.name, self.name)
        self.metadata_id = met['id']

    def _product_type(self):
        raise NotImplementedError

    def _product_extractor(self):
        raise NotImplementedError

    def transferring(self):
        if self._met_handler:
            self._met_handler.set_product_transferring(self.metadata_id)
        else:
            raise ProductError("Metadata handler not set for %s" % self.name)

    def received(self):
        if self._met_handler:
            self._met_handler.set_product_received(self.metadata_id)
        else:
            raise ProductError("Metadata handler not set for %s" % self.name)

    def failed(self):
        if self._met_handler:
            self._met_handler.set_product_failed(self.metadata_id)
        else:
            raise ProductError("Metadata handler not set for %s" % self.name)

    def transfer_status(self):
        if self._met_handler:
            self.met_handler.get_transfer_status()

    def product_metadata(self, product_location):
        extractor_class = self._product_extractor()
        met_extractor = extractor_class(product_location)
        met_extractor.extract_metadata()
        if self._met_handler:
            self._met_handler.add_prod_met(self.metadata_id, met_extractor.metadata)
        else:
            raise ProductError("Metadata handler not set for %s" % self.name)

    def add_ref_original(self, product_refs):
        if self._met_handler:
            self._met_handler.add_ref_original(self.metadata_id, product_refs)
        else:
            raise ProductError("Metadata handler not set for %s" % self.name)

    def add_ref_datastore(self, product_refs):
        if self._met_handler:
            self._met_handler.add_ref_datastore(self.metadata_id, product_refs)
        else:
            raise ProductError("Metadata handler not set for %s" % self.name)

class StreamProduct(ProductBase):
    """docstring for StreamProduct"""
    def __init__(self, head, stream, **kwargs):
        super(StreamProduct, self).__init__(stream, **kwargs)
        self.head = head
        self.stream = stream
        rdb = self.stream.replace('-','_') + '.rdb'
        rdb_full = self.stream.replace('-','_') + '.full.rdb'
        self.rdbs = [rdb, rdb_full]
        self.rdb_regex = '^{}$|^{}$'.format(*list(map(re.escape, self.rdbs)))
        self.rdb_writing_regex = '^%s$'% (self.stream.replace('-','_') + '\.writing\.' + '*.\.rdb')
        self.npy_regex = '^[a-z_]*.\/[0-9_]*.\.npy$'
        self.npy_writing_regex = '^[a-z_]*.\/[0-9_]*.\.writing.npy$'
        self.complete_token = 'complete'
        self.failed_token = 'failed'

    def _product_type(self):
        """Given a stream name we need to detect they type for creating metadata for stream products.
        Uses STREAM_TYPES to detect. Supports products of the format:
            (1) 1234567890-sdp-0 == MeerKATTelescopeProduct
            (2) 1234567890-sdp-0-continuum == MeerKATTelescopeProduct
            (3) 1234567890-sdp-1-flags == MeerKATFlagProduct
            (4) 1234567890-sdp-1-flags-continumm == MeerKATFlagProduct

        Parameters
        ----------
        stream_name: string : the name of the stream to detect.
        """
        stream_type = None
        for s_key in STREAM_TYPES.keys():
            if re.match(STREAM_TYPES[s_key], self.name):
                stream_type = s_key
                break
        if not stream_type:
            raise ProductError('No product type for %s' % (self.name))
        return stream_type

    def _product_extractor(self):
        stream_type = stream_type_detection(self.name)
        met_extractor = None
        if stream_type == 'MeerKATTelescopeProduct':
            met_extractor = MeerKATTelescopeProductMetExtractor
        elif stream_type == 'MeerKATFlagProduct':
            met_extractor = MeerKATFlagProductMetExtractor
        else:
            raise ProductError('No met extractor for %s' % (self.name))
        return met_extractor

