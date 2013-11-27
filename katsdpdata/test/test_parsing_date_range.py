import unittest
import os
import time
from katsdpdata import KatSdpData

class TestParsingDateRange(unittest.TestCase):
	def setUp(self):
		self.sdptest = KatSdpData('junk')

	def test__SAST_to_ISO8601(self):
		test_input = '1/1/2001 00:00:00 SAST'
		test_result = self.sdptest._SAST_to_ISO8601(test_input)
		self.assertEqual(test_result, '2000-12-31T22:00:00Z')

	def test__date_to_ISO8601(self):
		test_input = '1/1/2001'
		test_result = self.sdptest._date_to_ISO8601(test_input)
		self.assertEqual(test_result, '2000-12-31T22:00:00Z')

	def test__parse_date_range(self):
		test_input = '1/1/2001 to 31/12/2001'
		test_result = self.sdptest._parse_date_range(test_input)
		self.assertEqual(test_result, '[2000-12-31T22:00:00Z TO 2001-12-30T22:00:00Z]')

		test_input = '[1/1/2001 to 31/12/2001]'
		test_result = self.sdptest._parse_date_range(test_input)
		self.assertEqual(test_result, '[2000-12-31T22:00:00Z TO 2001-12-30T22:00:00Z]')

		test_input = 'now-7day to now'
		test_result = self.sdptest._parse_date_range(test_input)
		self.assertEqual(test_result, '[NOW-7DAY TO NOW]')
