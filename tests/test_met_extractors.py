"""Tests for the met extractor."""
# import pytest

from katsdpdata.met_extractors import \
    file_mime_detection, \
    MeerKATTelescopeProductMetExtractor


def test_met_integration_time():
    """
    Assert that the newly added Time on Target field is there, the same length
    as the Target list and the correct value.
    """
    met_extractor = file_mime_detection('./tests/1testdata_sdp_l0.rdb')
    met_extractor.extract_metadata()
    ## did we get the correct MeerKAT MetExtractor
    assert isinstance(met_extractor, MeerKATTelescopeProductMetExtractor)
    ## Integration Time (Time on Target)
    assert 'IntegrationTime' in met_extractor.metadata
    assert (
        len(met_extractor.metadata['Targets']) ==
        len(met_extractor.metadata['IntegrationTime']))
    assert met_extractor.metadata['IntegrationTime'][0] == '2390'

