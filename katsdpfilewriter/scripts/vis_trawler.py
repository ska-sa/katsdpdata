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
import socket
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

#cut and paste with modification from katsdpmetawriter/scripts/meta_writer.py
def make_boto_dict(s3_args):
    """Create a dict of keyword parameters suitable for passing into a boto.connect_s3 call using the supplied args."""
    return {
            "host": s3_args.s3_host,
            "port": s3_args.s3_port,
            "is_secure": False,
            "calling_format": boto.s3.connection.OrdinaryCallingFormat()
           }

#cut and paste with modification from katsdpmetawriter/scripts/meta_writer.py
def get_s3_connection(boto_dict, fail_on_boto=False):
    """Test the connection to S3 as described in the args, and return
    the current user id and the connection object.
    In general we are more concerned with informing the user why the
    connection failed, rather than raising exceptions. Users should always
    check the return value and make appropriate decisions.
    If set, fail_on_boto will not suppress boto exceptions. Used when verifying
    credentials.
    Returns
    -------
    s3_conn : S3Connection
        A connection to the s3 endpoint. None if a connection error occurred.
    """
    s3_conn = boto.connect_s3(**boto_dict)
    try:
        s3_conn.get_canonical_user_id()
         # reliable way to test connection and access keys
        return s3_conn
    except socket.error as e:
        logger.error("Failed to connect to S3 host {}:{}. Please check network and host address. ({})".format(s3_conn.host, s3_conn.host.port, e))
    except boto.exception.S3ResponseError as e:
        if e.error_code == 'InvalidAccessKeyId':
            logger.error("Supplied access key {} is not for a valid S3 user.".format(s3_conn.access_key))
        if e.error_code == 'SignatureDoesNotMatch':
            logger.error("Supplied secret key is not valid for specified user.")
        if e.status == 403 or e.status == 409:
            logger.error("Supplied access key ({}) has no permissions on this server.".format(s3_conn.access_key))
        if fail_on_boto:
            raise
    return None

def s3_create_bucket(s3_conn, bucket_name):
    """Create an s3 bucket, if it fails on a 403 or 409 error, print an error 
    message and reraise the exception. 
    Returns
    ------
    s3_bucket : boto.s3.bucket.Bucket
        An S3 Bucket object
    """
    try:
        s3_bucket = s3_conn.create_bucket(bucket_name)
    except boto.exception.S3ResponseError as e:
        if e.status == 403 or e.status == 409:
            logger.error("Error status {}. Supplied access key ({}) has no permissions on this server.".format(e.status, s3_conn.access_key))
        raise
    return s3_bucket

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
