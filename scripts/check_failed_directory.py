#!/usr/bin/env python
import re
import os
import katdal
import katsdpdata
import logging
from datetime import datetime, timedelta

from optparse import OptionParser

logging.getLogger("katdal").setLevel(logging.ERROR)

default_dirs = []
default_dirs.append('/var/kat/archive/data/ftp/failed')
default_dirs.append('/var/kat/archive2/data/ftp/failed')

wrapper = '<html><head><link type="text/css" href="http://kat-archive.kat.ac.za:8080/archive_search/css/ska_style.css" rel="stylesheet"><title>%s</title></head><body><div align="center">%s</div></body></html>'

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
    parser.add_option('--hours', type=float,
         help='Only look at the last .. hours.')
    parser.add_option('--web', action='store_true', default=False,
         help='Output as html')
    (options, args) = parser.parse_args()

    return options

def perform_checks(failed_dir, cutoff=None):
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
        ctime = datetime.fromtimestamp(os.path.getctime(h5))
        if cutoff and ctime < cutoff:
           continue 
        fsize = os.path.getsize(h5)
        try:
            k = katdal.open(h5)
            katdal_pass.append(h5)
        except Exception, e:
            e = repr(e)
            if katdal_errors.has_key(e):
                katdal_errors[e].append((ctime.isoformat(), os.path.basename(h5), fsize),)
            else:
                katdal_errors[e] = [(ctime.isoformat(), os.path.basename(h5), fsize),]

    met_extractor_errors = {}
    met_extractor_pass = []
    for h5 in katdal_pass:
        ctime = datetime.fromtimestamp(os.path.getctime(h5))
        fsize = os.path.getsize(h5)
        try:
            km = katsdpdata.met_extractors.TelescopeProductMetExtractor.factory(h5)
            met_extractor_pass.append(km.katfile)
        except Exception, e:
            e = repr(e)
            if met_extractor_errors.has_key(e):
                met_extractor_errors[e].append((ctime.isoformat(), os.path.basename(h5), fsize),)
            else:
                met_extractor_errors[e] = [(ctime.isoformat(), os.path.basename(h5), fsize),]
    return katdal_errors, met_extractor_errors, met_extractor_pass


def html_results(failed_dir, katdal_errors, met_extracor_errors, met_extractor_pass):
    with open(os.path.join(failed_dir, 'index.html'), 'w') as index:
        body = html_body(katdal_errors, met_extracor_errors, met_extractor_pass)
        html = wrapper % (failed_dir, '\n'.join(body))
        index.write(html)
  
def html_body(katdal_errors, met_extractor_errors, met_extractor_pass):
    th = '<tr><th>%s</th></tr>'
    td = '<tr><td>%s</td/></tr>'
    tb_start = '<table width="950" cellspacing="10" cellpadding="0" border="0" bgcolor="#ffffff">'
    tb_end = '</table>'
    body = []
    body.append(tb_start)
    body.append(th % 'katdal errors')
    body.append(tb_end)
    for key,val in katdal_errors.iteritems():
        body.append(tb_start)
        body.append(th % key)
        for v in sorted(val, reverse=True):
            body.append(td % ('%s: %s %i bytes' % (v[0], v[1], v[2])))
        body.append(tb_end) 
    
    body.append(tb_start)
    body.append(th % 'TelescopeProductMetExtractor errors')
    bodr.append(tb_end)
    for key,val in met_extractor_errors.iteritems():
        body.append(tb_start) 
        body.append(th % key)
        for v in sorted(val, reverse=True):
            body.append(td % ('%s: %s %i bytes' % (v[0], v[1], v[2])))
        body.append(tb_end)

    body.append(tb_start)
    body.append(th % 'Passed all test')
   
    if met_extractor_pass:
        body.append('\n'.join(td % (met_extractor_pass)))
    else:
        body.append(td % 'No files remaining.')
    body.append(tb_end)
    return body

opts = get_options()

failed_dirs = []
if opts.dir:
    failed_dirs.append(opts.dir)
if opts.archive:
    failed_dirs.append(default_dirs[0])
if opts.archive2:
    failed_dirs.append(default_dirs[1])
if opts.hours:
    cutoff = datetime.now() - timedelta(hours=opts.hours)
else:
    cutoff = None

for failed_dir in failed_dirs:
    katdal_errors, met_extractor_errors, met_extractor_pass = perform_checks(failed_dir, cutoff)
    if opts.web:    
        html_results(failed_dir, katdal_errors, met_extractor_errors, met_extractor_pass)
    else:
        print html_body(katdal_errors, met_extractor_errors, met_extractor_pass)
