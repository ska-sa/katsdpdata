#!/usr/bin/env python
import os
import sys
import katdal

from katsdpdata.met_extractors import RTSTelescopeProductMetExtractor
from optparse import OptionParser

usage = 'Usage: %prog katfile'
parser = OptionParser(usage=usage)
(options, args) = parser.parse_args()

if (len(sys.argv) == 2):
    filename = sys.argv[1]
else:
    print parser.format_help()
    sys.exit(0)

metfilename = '%s.%s' % (filename, 'met')
if os.path.isfile(metfilename):
    print 'Metadate file %s already exists.' % (metfilename)
    sys.exit(0)

#met extractor specific
katdata = katdal.open(filename)
met_extractor = RTSTelescopeProductMetExtractor(katdata)
met_extractor.extract_metadata()
met_extractor.write_metadatafile()
