import unittest
import os
import time
from katsdpdata.met_extractors import MeerKATAR1TelescopeProductMetExtractor

class FakeFile(object):
    """docstring for FakeFile"""
    def __init__(self):
        super(FakeFile, self).__init__()
        self.filename = '01234567890.h5'
        
class FakeKatData(object):
    """docstring for FakeKatData"""
    def __init__(self):
        super(FakeKatData, self).__init__()
        self.file = FakeFile()
        self.obs_params ={}
        self.obs_params['script_arguments'] = "run-obs-script \
/home/kat/katsdpscripts/AR1/observations/interferometric_pointing.py \
'/home/kat/katsdpscripts/AR1/catalogues/gaincal_l_5jy.csv' \
--horizon=20 -t 40 -m 21600 --description='MKAIV-202 Interferometric Pointing Calibration' \
--proposal-id='MKAIV-202' --program-block-id='MKAIV-202' --sb-id-code=20160908-0004 \
--issue-id='sample issue id' --proposal-description='sample proposal description'"

class TestExtractorsForMeerKATAR1TelescopeProducts(unittest.TestCase):
     def setUp(self):
         self.test_class = MeerKATAR1TelescopeProductMetExtractor(FakeKatData()) 
         self.test_class._extract_metadata_for_project()

     def test_extract_for_project(self):
         self.assertEqual(self.test_class.metadata['ProposalId'], 'MKAIV-202')
         self.assertEqual(self.test_class.metadata['ProgramBlockId'], 'MKAIV-202')
         self.assertEqual(self.test_class.metadata['ScheduleBlockIdCode'], '20160908-0004')
         self.assertEqual(self.test_class.metadata['IssueId'], 'sample issue id')
         self.assertEqual(self.test_class.metadata['ProposalDescription'], 'sample proposal description')

