"""Tests for the met extractor."""
import pytest
from os.path import exists
from pathlib import Path, PurePath
from katsdpdata.prod_handler import ProductFactory, RDBProduct, L0Product, L1Product
from katsdpdata.met_handler import MetaDataHandler


class STUB_Res:
    def __init__(self):
        self.hits = 0
        self.doc = {}


class STUB_Solr:
    def __init__(self, *args, **kwargs):
        self._add = []

    def search(self, query):
        res = STUB_Res()
        if not self._add:
            return res
        res.doc = self._add[-1]
        res.hits = 1
        return res

    def add(self, data):
        self._add.append(data[0])

    def history(self):
        return self._add

    def __getattr__(self, name):
        def method(*args):
            print("solr: " + name + " : ", str(args))
        return method


class STUB_MetaDataHandler(MetaDataHandler):
    def __init__(self, solr_url, product_type, product_name, product_id='dummy_sdp_l0'):
        self.solr_url = 'dummy_url'
        self.solr = STUB_Solr()
        self.product_type = 'test_product_type'
        self.product_name = 'test_product_name'
        self.product_id = product_id

    def create_core_met(self):
        new_met = {}
        new_met['id'] = self.product_id
        new_met['CAS.ProductId'] = self.product_id
        new_met['CaptureStreamId'] = self.product_id
        new_met['CAS.ProductName'] = self.product_name
        new_met['CAS.ProductTypeId'] = 'urn:kat:{}'.format(self.product_type)
        new_met['CAS.ProductTypeName'] = self.product_type
        new_met['id'] = self.product_id
        self.solr.add([new_met])
        return new_met

    def get_prod_met(self, key=None):
        return self.solr.search(key).doc


@pytest.fixture(scope="class")
def test_trawl_dir():
    test_dir = Path(__file__).parent.absolute()
    return PurePath(test_dir, 'test_data_directory')


@pytest.fixture(scope="class")
def rdb_product_dir():
    test_dir = Path(__file__).parent.absolute()
    return PurePath(test_dir, 'test_data_directory', '1234567891')


class TestProductFactory:
    def test_product_factory_product_detection(self, test_trawl_dir):
        """ Assert that the products are being detected correctly.
        """
        if not exists('/home/katsdpdata/tests/test_data_directory'):
            return
        pf = ProductFactory(test_trawl_dir)
        assert len(pf.get_rdb_products()) == 2
        assert len(pf.get_l0_products()) == 1
        assert len(pf.get_l1_products()) == 1

    def test_product_factory_pruning(self, test_trawl_dir):
        """ Assert that the products are being detected correctly.
        """
        if not exists('/home/katsdpdata/tests/test_data_directory'):
            return
        pf = ProductFactory(test_trawl_dir)
        pf.prune_rdb_products()
        assert len(pf.get_rdb_products()) == 1
        assert len(pf.get_l0_products()) == 1
        assert len(pf.get_l1_products()) == 1


class TestRDBProduct:
    def test_set_rdb_metadata(self, rdb_product_dir):
        if not exists('/home/katsdpdata/tests/test_data_directory'):
            return
        product = RDBProduct(str(rdb_product_dir))
        product.met_handler = STUB_MetaDataHandler
        assert product.mh().solr.search('last').hits == 0
        product.discover_trawl_files()
        product.metadata_when_created()
        assert product.mh().solr.search('last').hits == 1
        for key in [
                'CAS.ProductTypeName', 'Antennas', 'CenterFrequency', 'ChannelWidth',
                'MinFreq', 'MaxFreq', 'Bandwidth', 'Description', 'Details',
                'DumpPeriod', 'Duration', 'ExperimentID', 'FileSize', 'KatfileVersion',
                'KatpointTargets', 'NumFreqChannels', 'Observer', 'RefAntenna',
                'StartTime', 'Targets', 'IntegrationTime', 'InstructionSet',
                'ProposalId', 'ProgramBlockId', 'ScheduleBlockIdCode',
                'CaptureBlockId', 'StreamId', 'CaptureStreamId', 'Prefix', 'DecRa',
                'ElAz']:
            assert key in product.mh().solr.search('last').doc.keys()


class TestL0Prefix:
    def test_l1_prefix(self):
        product = L1Product('/data/1234567890-sdp-l1-flags')
        assert product.prefix == '1234567890-sdp-l1-flags'
        mh = product.mh()
        assert mh.prefix == '1234567890-sdp-l1-flags'

    def test_l0_prefix(self, rdb_product_dir):
        product = L0Product('/data/1234567890-sdp-l0')
        assert product.prefix == '1234567890-sdp-l0'
        mh = product.mh()
        assert mh.prefix == '1234567890-sdp-l0'
