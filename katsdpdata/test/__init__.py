"""Unit test suite for katsdpdata"""

import unittest
import test_extractors

def suite():
    loader = unittest.TestLoader()
    testsuite = unittest.TestSuite()
    testsuite.addTests(loader.loadTestsFromModule(test_extractors))
    return testsuite

if __name__ == '__main__':
        unittest.main(defaultTest='suite')
