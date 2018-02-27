#!/usr/bin/env python

"""Very crude parallel file uploader to trawl NPY files into S3."""

import concurrent.futures
import glob
import itertools
import logging
import multiprocessing
import os
import re
import sys
import time

from optparse import OptionParser

import boto
import boto.s3.connection
import numpy as np

import katsdpservices

GLOB = '*.npy'
CBID_REGEX = '^[0-9]{9}.*$'
MAX_TRANSFER = 1000


def main():
    cbid_dirs = [d.path for d in os.scandir(trawl_dir)
                 if d.is_dir() and re.match(CBID_REGEX, os.path.relpath(d.path, trawl_dir))]
    #TODO: add checks for completed cbids
    trawl_keys = [os.path.relpath(d, trawl_dir) for d in cbid_dirs]
    trawl_vals = [glob.glob(os.path.join(d,'**',GLOB), recursive=True) for d in cbid_dirs]
    uploads = dict(zip(trawl_keys, trawl_vals))
    file_list = list(itertools.chain(*[uploads[i]
                     for i in sorted(uploads.keys())]))[0:MAX_TRANSFER]
    upload_size = sum(os.path.getsize(f)
                      for f in file_list if os.path.isfile(f))
    if upload_size > 0:
        logger.info("Uploading {} MB of data".format(upload_size / 1e6))
        log_time = {}
        results = parallel_upload(file_list, x, log_time=log_time)
        #TODO: check results for completion and exceptions
        logger.info("Upload complete in {}s ({} MBps) - Core multiplier {}".
                    format(log_time['PARALLEL_UPLOAD'], upload_size / 1e6 / log_time['PARALLEL_UPLOAD'], x))
    else:
         logger.info("No data to upload ({} MB)".format(upload_size / 1e6))


def transfer_files(file_list):
    conn = boto.connect_s3(host=s3_host, port=s3_port, is_secure=False,
                           calling_format=boto.s3.connection.OrdinaryCallingFormat())
    logger.debug("Connected on {}".format(conn))
    bucket = None
    for filename in file_list:
        bucket_name, key_name = os.path.relpath(filename, trawl_dir).split('/',1)
        key_name = os.path.splitext(key_name)[0]
        if not bucket or bucket.name != bucket_name:
            bucket = conn.create_bucket(bucket_name)
        key = bucket.new_key(key_name)
        key.set_contents_from_string(np.load(filename).tobytes())
    # logger.info("Process uploaded {} keys".format(len(file_list)))


def timeit(func):
    """Taken from an example from the internet."""
    def wrapper(*args, **kwargs):
        ts = time.time()
        result = func(*args, **kwargs)
        te = time.time()
        if 'log_time' in kwargs:
            name = kwargs.get('log_name', func.__name__.upper())
            kwargs['log_time'][name] = int(te - ts)
        else:
            logger.info(('{} {} ms').format(func.__name__, (te - ts)))
        return result
    return wrapper


@timeit
def parallel_upload(file_list, x, **kwargs):
    workers = x * multiprocessing.cpu_count()
    logger.info("Using {} workers".format(workers))
    files = [file_list[i::workers] for i in range(workers)]
    logger.info("Processing {} files".format(len(file_list)))
    future = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        for f in files:
            future.append(executor.submit(transfer_files, f))
        executor.shutdown(wait=True)
    return [f.result() for f in future]


if __name__ == "__main__":
    katsdpservices.setup_logging()
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("katsdpvistrawler")
    katsdpservices.setup_restart()

    parser = OptionParser(usage="vis_trawler.py <trawl_directory>")
    parser.add_option('--s3-host', default='localhost',
                        help='S3 gateway host address [default = %default]')
    parser.add_option('--s3-port', default=7480,
                        help='S3 gateway port [default = %default]')
    parser.add_option('-m', '--multiplier', default=2,
                        help='Workers == m*num_cores [default = %default]')
    (options, args) = parser.parse_args()
    if len(args) < 1 or not os.path.isdir(args[0]):
        print(__doc__)
        sys.exit()

    #set global arguments
    trawl_dir = args[0]
    x = int(options.multiplier)
    s3_host = options.s3_host
    s3_port = options.s3_port
    main()
