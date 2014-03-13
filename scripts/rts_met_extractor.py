#!/usr/bin/env python
import katdal
import os
import sys
import subprocess
import time

from katsdpdata.met_extractors import RTSMetExtractor
from optparse import OptionParser
from xml.etree import ElementTree

usage = 'Usage: %prog filename'
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
katdata = katdal.open(os.path.abspath(filename))
major_version = int(float(katdata.version))
met_extractor = RTSMetExtractor(katdata)
met_extractor.set_metadata()

with open(metfilename, 'w') as metfile:
   metfile.write(met_extractor.get_metadata())

