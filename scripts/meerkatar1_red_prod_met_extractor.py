#!/usr/bin/env python
import os
import sys

from katsdpdata.met_extractors import MeerKATAR1ReductionProductMetExtractor
from katsdpdata.met_extractors import CalibrationProductMetExtractor
from optparse import OptionParser

usage = 'Usage: %prog product_name'
parser = OptionParser(usage=usage)
(options, args) = parser.parse_args()

if (len(sys.argv) == 2):
    product_name = os.path.normpath(sys.argv[1])
else:
    print parser.format_help()
    sys.exit(0)

metfilename = '%s.%s' % (product_name, 'met')
if os.path.isfile(metfilename):
    print 'Metadate file %s already exists.' % (product_name)
    sys.exit(0)

#met extractor specific
if product_name[:10] == "MeerKATAR1":
    print "reduction"
    met_extractor = MeerKATAR1ReductionProductMetExtractor(product_name)
else:
    print "cal"
    met_extractor = CalibrationProductMetExtractor(product_name)
met_extractor.extract_metadata()
met_extractor.write_metadatafile()
