#!/usr/bin/env python
import re
import os
import sys
import katdal
import katsdpdata
import logging
import time
from datetime import datetime, timedelta

from optparse import OptionParser

logging.getLogger("katdal").setLevel(logging.ERROR)

htaccess_file = '.htaccess'

def get_options():
    """Sets options from the arguments passed to the script.
    Returns
    -------
    options: (Boolean, optparse.Values)
        Options and arguments.
    """

    usage = 'usage: %prog [options]'

    parser = OptionParser(usage=usage)
    parser.add_option('--dirs', type='str',
         help='Comma seperated list of directories to check.')
    parser.add_option('--hours', type=float,
         help='Only look at the last number of hours requested. Does not apply to daemon mode.')
    parser.add_option('--daemon', action='store_true', default=False,
         help='Loop forever and update %s' % (htaccess_file))
    (options, args) = parser.parse_args()

    return options

def get_h5files(directory):
    return [os.path.join(directory,f) for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f)) and re.match('[0-9]{10}.h5$', f)]

def filter_htaccess_h5files(directory, htfile, h5files):
    keep_descriptions = []
    try:
        with open(htfile, 'r') as htaccess:
            info = htaccess.read()
            for i in info.split('\n'):
                if re.match('AddDescription.*[0-9]{10}.h5$', i):
                    i_file = re.findall('[0-9]{10}.h5$', i)[0]
                    if os.path.join(directory, i_file) in h5files:
                        h5files.remove(os.path.join(directory, i_file))
                        keep_descriptions.append(i)
    except IOError:
        print 'No %s in %s' % (htaccess_file, directory)
    return h5files, keep_descriptions

def filter_cutoff(cutoff_time, h5files):
    for h5 in h5files:
        ctime = datetime.fromtimestamp(os.path.getctime(h5))
        if cutoff_time and ctime < cutoff_time:
            h5files.remove(h5)
    return h5files

def move_md5s(failed_dir, h5files):
    staging_dir = os.path.join(os.path.split(os.path.normpath(failed_dir))[0], 'staging')
    for h5 in h5files:
        h5_md5 = os.path.basename(h5) + '.md5'
        if os.path.isfile(os.path.join(staging_dir, h5_md5)):
            os.rename(os.path.join(staging_dir, h5_md5), os.path.join(failed_dir, h5_md5))

def check_katdal(h5files):
    katdal_errors = {}
    katdal_pass = []
    for h5 in h5files:
        ctime = datetime.fromtimestamp(os.path.getctime(h5))
        fsize = os.path.getsize(h5)
        try:
            k = katdal.open(h5)
            harass_k = str(k) #test for more fails
            katdal_pass.append(h5)
        except Exception, e:
            e = repr(e).replace('"','\'')
            if katdal_errors.has_key(e):
                katdal_errors[e].append((ctime.isoformat(), os.path.basename(h5), fsize),)
            else:
                katdal_errors[e] = [(ctime.isoformat(), os.path.basename(h5), fsize),]
    return katdal_errors, katdal_pass

def check_met_extractor(h5files):
    met_extractor_errors = {}
    met_extractor_pass = []
    for h5 in h5files:
        ctime = datetime.fromtimestamp(os.path.getctime(h5))
        fsize = os.path.getsize(h5)
        try:
            km = katsdpdata.met_extractors.FileBasedTelescopeProductMetExtractor.factory(h5)
            met_extractor_pass.append(km.katfile)
        except Exception, e:
            e = repr(e)
            if met_extractor_errors.has_key(e):
                met_extractor_errors[e].append((ctime.isoformat(), os.path.basename(h5), fsize),)
            else:
                met_extractor_errors[e] = [(ctime.isoformat(), os.path.basename(h5), fsize),]
    return met_extractor_errors, met_extractor_pass

def create_descriptions(error_type, errors):
    descriptions = []
    for key,val in errors.iteritems():
        for v in sorted(val, reverse=True):
            descriptions.append('AddDescription "%s: %s" %s' % (error_type, key, v[1]))
    return descriptions

def write_htaccess(htfile, descriptions):
    with open(htfile, 'w') as htaccess:
        htaccess.write('IndexOptions FancyIndexing\n')
        htaccess.write('IndexIgnore *.md5\n')
        htaccess.write('AddDescription "CAS metadata" *.met\n')
        htaccess.write('\n'.join(descriptions))

def update_h5files_htaccess_descriptions(directory):
    h5files = get_h5files(directory)
    h5files, descriptions = filter_htaccess_h5files(directory, os.path.join(directory, htaccess_file), h5files)
    move_md5s(directory,h5files)
    katdal_errors, katdal_pass = check_katdal(h5files)
    met_extractor_errors, met_extractor_pass = check_met_extractor(katdal_pass)
    descriptions.extend(create_descriptions('katdal error', katdal_errors))
    descriptions.extend(create_descriptions('met error', met_extractor_errors))
    return descriptions

def update_htaccess(default_dirs):
    for d in default_dirs:
        descriptions = []
        descriptions.extend(update_h5files_htaccess_descriptions(d))
        write_htaccess(os.path.join(d,htaccess_file), descriptions)

     
def show_h5_results(directory, katdal_errors, met_extractor_errors, met_extractor_pass):
    print directory
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

def run_daemon(default_dirs):
    while True:
        print 'Updating...'
        update_htaccess(default_dirs)
        print 'done.'
        time.sleep(10)

def run_main(default_dirs, cutoff):
    for d in default_dirs:
        h5files = get_h5files(d)
        h5files = filter_cutoff(cutoff, h5files)
        katdal_errors, katdal_pass = check_katdal(h5files)   
        met_errors, met_pass = check_met_extractor(katdal_pass)  
        show_h5_results(d, katdal_errors, met_errors, met_pass)         

if __name__ == '__main__':
    opts = get_options()
    default_dirs = [d.strip(' ') for d in opts.dirs.split(',')]

    if opts.daemon:
        run_daemon(default_dirs)
        sys.exit()

    if opts.hours:
        cutoff = datetime.now() - timedelta(hours=opts.hours)
    else:
        cutoff = None
    run_main(default_dirs, cutoff)

