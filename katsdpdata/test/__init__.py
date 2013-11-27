"""Unit test suite for katsdpdata"""

import unittest
import test_parsing_date_range

def suite():
    loader = unittest.TestLoader()
    testsuite = unittest.TestSuite()
    testsuite.addTests(loader.loadTestsFromModule(test_parsing_date_range))
    return testsuite

if __name__ == '__main__':
        unittest.main(defaultTest='suite')
