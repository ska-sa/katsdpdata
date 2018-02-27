#!/usr/bin/env python

"""Very crude parallel file uploader to trawl NPY files into S3."""

import os
import sys
import time
import glob
import multiprocessing
import katsdpservices
import logging

from optparse import OptionParser

import boto
import boto.s3.connection
import numpy as np

S3_HOST = '10.98.56.16'
S3_PORT = 7480
X = 1

def main(directory):
    upload_size = sum(os.path.getsize(f)
                      for f in glob.glob('{}/*/*'.format(directory))
                      if os.path.isfile(f)) / 1e6
    logger.info("Uploading {} MB of data".format(upload_size))
    st = time.time()
    parallel_upload(directory, X)
    et = time.time() - st
    logger.info("Upload complete in {}s ({} MBps) - Core multiplier {}".format(et, upload_size / et, X))


def transfer_files(file_list):
    conn = boto.connect_s3(host=S3_HOST, port=S3_PORT, is_secure=False,
                           calling_format=boto.s3.connection.OrdinaryCallingFormat())
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
        if 'log_time' in kw:
            name = kw.get('log_name', func.__name__.upper())
            kw['log_time'][name] = int(te - ts)
        else:
            print('{} {} ms').format(func.__name__, (te - ts))
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
    logger = logging.getLogger("katsdpvistrawler")
    katsdpservices.setup_restart()

    parser = OptionParser(usage="vis_trawler.py <capture_stream_directory>")
    (options, args) = parser.parse_args()
    if len(args) < 1:
        print(__doc__)
        sys.exit()
    main(args[0])
