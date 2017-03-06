import re
import os
import katdal
import katsdpdata
import time
import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

failed_dir = '/var/kat/archive/data/ftp/failed/'
h5files = [os.path.join(failed_dir,f) for f in os.listdir(failed_dir) if os.path.isfile(os.path.join(failed_dir, f)) and re.match('[0-9]{10}.h5$', f)]

katdal_errors = {}
katdal_pass = []
for h5 in h5files:
    ctime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getctime(h5)))
    fsize = os.path.getsize(h5)
    try:                                                                                                           
        k = katdal.open(h5)
        katdal_pass.append(h5)
    except Exception, e:
        e = repr(e)
        if katdal_errors.has_key(e):
            katdal_errors[e].append((ctime, os.path.basename(h5), fsize),)
        else:
            katdal_errors[e] = [(ctime, os.path.basename(h5), fsize),]

met_extractor_error = {}
met_extractor_pass = []
for h5 in katdal_pass:
    try:                                                                                                           
        km = katsdpdata.met_extractors.TelescopeProductMetExtractor.factory(h5)
        met_extractor_pass.append(km)
    except Exception, e:
        e = repr(e)
        if met_extractor_error.has_key(e):
            met_extractor_error[e].append((ctime, os.path.basename(h5), fsize),)
        else:
            met_extractor_error[e] = [(ctime, os.path.basename(h5), fsize),]

title = 'KATDAL ERROR'
print title
print '+'*len(title)
for key,val in katdal_errors.iteritems():
    print key
    print '='*len(key)
    for v in sorted(val, reverse=True):
        print '%s: %s %i bytes' % (v[0], v[1], v[2])
print '+'*len(title) + '\n'
 
title = 'MET EXTRACTOR ERROR'
print title
print '+'*len(title)
for key,val in met_extractor_error.iteritems():
    print key
    print '='*len(key)
    for v in sorted(val, reverse=True):
        print '%s: %s %i bytes' % (v[0], v[1], v[2])
print '+'*len(title) + '\n'

title = 'FILES PASSING'
print title
print '+'*len(title)
if met_extractor_pass:
    print '\n'.join(met_extractor_pass)
else:
    print 'No files remaining.'
print '+'*len(title) + '\n'

