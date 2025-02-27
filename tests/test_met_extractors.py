"""Tests for the met extractor."""
from os.path import exists
from katsdpdata.met_extractors import \
    file_mime_detection, \
    MeerKATTelescopeProductMetExtractor

class TestTelescopeProductExtractor:

    def setup_class(cls):
        cls.met = file_mime_detection('./tests/1testdata_sdp_l0.rdb')
        cls.met.extract_metadata()
        assert isinstance(cls.met, MeerKATTelescopeProductMetExtractor)

    def test_basic_metadata(self):
        metadata = self.met.metadata
        # metadata_product_type
        assert metadata['CAS.ProductTypeName'] == 'MeerKATTelescopeProduct'
        # metadata_for_project
        assert metadata['ProposalId'] == 'COM-20180918-TF-01'
        assert metadata['ProgramBlockId'] == ''
        assert metadata['ScheduleBlockIdCode'] == '20201125-0014'
        assert 'IssueId' not in metadata
        assert 'ProposalDescription' not in metadata
        # metadata_for_capture_stream
        assert metadata['CaptureBlockId'] == '1606356963'
        assert metadata['StreamId'] == 'sdp_l0'
        assert metadata['CaptureStreamId'] == '1606356963_sdp_l0'
        assert metadata['Prefix'] == '1606356963'

    def test_extended_metadata(self):
        metadata = self.met.metadata
        # metadata_from_katdata
        assert len(metadata['Antennas']) == 59  # number of antennas
        assert metadata['Antennas'][0] == 'm000'
        assert metadata['CenterFrequency'] == '1283791015.62'
        assert metadata['ChannelWidth'] == '208984.375'
        assert metadata['MinFreq'] == '856000000.0'
        assert metadata['MaxFreq'] == '1712000000.0'
        assert metadata['Bandwidth'] == '856000000.0'
        assert metadata['Description'] == \
            'Full-Stokes imaging of MeerKAT gain calibrators (L) LST 06 - 12 (redo)'
        assert metadata['Details'].startswith('=====')
        assert metadata['DumpPeriod'] == '7.9966'
        assert metadata['Duration'] == '15065.63'
        assert metadata['ExperimentID'] == '20201125-0014'
        assert metadata['FileSize'] == '437083176960'
        assert metadata['KatfileVersion'] == '4.0'
        assert len(metadata['KatpointTargets']) == 33  # number of targets
        assert metadata['KatpointTargets'][0] == \
            'J0408-6545 | 0408-658, radec fluxcal bpcal delaycal, 4:08:20.38, -65:45:09.1'
        assert metadata['NumFreqChannels'] == '4096'
        assert metadata['Observer'] == 'Sam Legodi'
        assert metadata['RefAntenna'] == 'array'
        assert metadata['StartTime'] == '2020-11-26T02:17:14Z'
        # XXX Consider fixing Targets to be as long as KatpointTargets (just use J-names)
        assert len(metadata['Targets']) == 66  # number of target names, including aliases
        assert metadata['Targets'][0] == 'J0408-6545'
        assert metadata['Targets'][1] == '0408-658'
        assert metadata['InstructionSet'].startswith('/usr/local/bin/astrokat-observe.py')

    def test_location_metadata(self):
        metadata = self.met.metadata
        # location_from_katdata
        assert len(metadata['DecRa']) == 38  # number of scans/tracks on radec targets
        assert metadata['DecRa'][0] == '-65.752528, 62.084917'
        # XXX Consider using both azel and radec coordinates (allowing for source motion)
        assert metadata['ElAz'] == []

    def test_met_integration_time(self):
        """
        Assert that the newly added Time on Target field is there, the same length
        as the Target list and the correct value.
        """
        metadata = self.met.metadata
        # Integration Time (Time on Target)
        assert 'IntegrationTime' in metadata
        assert len(metadata['Targets']) == len(metadata['IntegrationTime'])
        assert metadata['IntegrationTime'][0] == '2390'
