#!/usr/bin/env python3

import sys

from katsdpdata.met_extractors import file_mime_detection
from optparse import OptionParser

usage = 'Usage: %prog katfile'
parser = OptionParser(usage=usage)
(options, args) = parser.parse_args()

if (len(sys.argv) == 2):
    filename = sys.argv[1]
else:
    print(parser.format_help())
    sys.exit(0)

# met extractor specific
met_extractor = file_mime_detection(filename)
met_extractor.extract_metadata()
met_extractor.write_metadatafile()
