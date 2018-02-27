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


def main(directory, c_start=1, c_range=8):
    upload_size = sum(os.path.getsize(f)
                      for f in glob.glob('{}/*/*'.format(directory))
                      if os.path.isfile(f)) / 1e6
    logger.info("Uploading {} MB of data".format(upload_size))
    for x in range(c_range):
        st = time.time()
        parallel_upload(directory, x+c_start)
        et = time.time() - st
        logger.info("Upload complete in {}s ({} MBps) - Core multiplier {}".format(et, upload_size / et, x+c_start))


def transfer_files(i, file_list):
    conn = boto.connect_s3(host='10.98.56.16', port=7480, is_secure=False,
                           calling_format=boto.s3.connection.OrdinaryCallingFormat())
    bucket = None
    for filename in file_list:
        bucket_name, key_name = filename.split('/', 1)
        key_name = os.path.splitext(key_name)[0]
        if not bucket or bucket.name != bucket_name:
            bucket = conn.create_bucket(bucket_name)
        key = bucket.new_key(key_name)
        key.set_contents_from_string(np.load(filename).tobytes())
    # logger.info("Process {} uploaded {} keys".format(i, len(file_list)))


def parallel_upload(directory, x):
    cores = x * multiprocessing.cpu_count()
    logger.info("Using {} cores".format(cores))

    all_files = glob.glob('{}/*/*'.format(directory))
    files = [all_files[i::cores] for i in range(cores)]
    logger.info("Processing {} files".format(len(all_files)))
    st = time.time()
    #with multimap(cores) as pmap:
    #    for _ in pmap(transfer_files,
    #                  ((i, file_list) for (i, file_list) in enumerate(files))):
    #        pass

    et = time.time() - st
    logger.info("Elapsed: {}".format(et))


if __name__ == "__main__":
    katsdpservices.setup_logging()
    logger = logging.getLogger("katsdpvistrawler")
    katsdpservices.setup_restart()

    parser = OptionParser(usage="vis_trawler.py <capture_stream_directory>")
    parser.add_option("-r", "--c_range", default=1,
                      help='Range of core multipliers to test. Default 1')
    parser.add_option("-s", "--c_start", default=7,
                      help='Starting multiplier for core testing. Default 7')
    (options, args) = parser.parse_args()
    if len(args) < 1:
        print(__doc__)
        sys.exit()
    main(args[0], c_start=int(options.c_start), c_range=int(options.c_range))
