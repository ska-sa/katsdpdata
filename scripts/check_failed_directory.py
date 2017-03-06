#!/usr/bin/env python
import re
import os
import katdal
import katsdpdata
import time
import logging
import sys

from optparse import OptionParser

logging.getLogger("katdal").setLevel(logging.ERROR)

default_dirs = []
default_dirs.append('/var/kat/archive/data/ftp/failed')
default_dirs.append('/var/kat/archive2/data/ftp/failed')



def get_options():
    """Sets options from the arguments passed to the script.
    Returns
    -------
    options: (Boolean, optparse.Values)
        Options and arguments.
    """

    usage = 'usage: %prog [options]'

    parser = OptionParser(usage=usage)
    parser.add_option('--dir', type='str',
         help='Directory to check. Use --archive --archive2 to check default directories.')
    parser.add_option('-a', '--archive', action='store_true', default=False,
         help='Check directory, /var/kat/archive/data/ftp/failed')
    parser.add_option('-b', '--archive2', action='store_true', default=False,
         help='Check directory, /var/kat/archive2/data/ftp/failed')
    (options, args) = parser.parse_args()

    return options

def perform_checks(failed_dir):
    h5files = [os.path.join(failed_dir,f) for f in os.listdir(failed_dir) if os.path.isfile(os.path.join(failed_dir, f)) and re.match('[0-9]{10}.h5$', f)]
    #move md5s
    if failed_dir in default_dirs:
        staging_dir = os.path.join(os.path.split(os.path.normpath(failed_dir))[0], 'staging') 
        for h5 in h5files:
            h5_md5 = os.path.basename(h5) + '.md5'
            if os.path.isfile(os.path.join(staging_dir, h5_md5)):
                os.rename(os.path.join(staging_dir, h5_md5), os.path.join(failed_dir, h5_md5))

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

    met_extractor_errors = {}
    met_extractor_pass = []
    for h5 in katdal_pass:
        ctime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getctime(h5)))
        fsize = os.path.getsize(h5)
        try:
            km = katsdpdata.met_extractors.TelescopeProductMetExtractor.factory(h5)
            met_extractor_pass.append(km.katfile)
        except Exception, e:
            e = repr(e)
            if met_extractor_errors.has_key(e):
                met_extractor_errors[e].append((ctime, os.path.basename(h5), fsize),)
            else:
                met_extractor_errors[e] = [(ctime, os.path.basename(h5), fsize),]
    return katdal_errors, met_extractor_errors, met_extractor_pass

def show_results(katdal_errors, met_extractor_errors, met_extractor_pass):
    title = 'KATDAL ERRORS'
    print title
    print '+'*len(title)
    for key,val in katdal_errors.iteritems():
        print key
        print '='*len(key)
        for v in sorted(val, reverse=True):
            print '%s: %s %i bytes' % (v[0], v[1], v[2])
    print '+'*len(title) + '\n'

    title = 'MET EXTRACTOR ERRORS'
    print title
    print '+'*len(title)
    for key,val in met_extractor_errors.iteritems():
        print key
        print '='*len(key)
        for v in sorted(val, reverse=True):
            print '%s: %s %i bytes' % (v[0], v[1], v[2])
    print '+'*len(title) + '\n'

    title = 'FILES STILL PASSING'
    print title
    print '+'*len(title)
    if met_extractor_pass:
        print '\n'.join(met_extractor_pass)
    else:
        print 'No files remaining.'
    print '+'*len(title) + '\n'

opts = get_options()

failed_dirs = []
if opts.dir:
    failed_dirs.append(opts.dir)
if opts.archive:
    failed_dirs.append(default_dirs[0])
if opts.archive2:
    failed_dirs.append(default_dirs[1])

for failed_dir in failed_dirs:
    katdal_errors, met_extractor_errors, met_extractor_pass = perform_checks(failed_dir)
    show_results(katdal_errors, met_extractor_errors, met_extractor_pass)
